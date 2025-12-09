#!/usr/bin/env python3
import json
import logging
import time
import traceback
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse
import mimetypes

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import pytz
import mysql.connector


# ------------- DATA CLASSES -------------

@dataclass
class DBConfig:
    host: str
    user: str
    password: str
    database: str
    port: int = 3306


@dataclass
class SourceConfig:
    mode: str
    rss_url: str
    full_content_from_article_page: bool
    article_content_selector: str
    title_selector: str
    timezone: str


@dataclass
class TargetConfig:
    name: str
    base_url: str
    username: str
    application_password: str
    default_status: str
    default_categories: List[int]
    default_tags: List[int]
    post_type: str = "posts"


@dataclass
class RuntimeConfig:
    poll_interval_seconds: int
    user_agent: str
    max_posts_per_cycle: int
    log_level: str


@dataclass
class PostItem:
    guid: str
    url: str
    title: str
    content_html: str
    published_at: Optional[datetime]


# ------------- DB LAYER (MySQL) -------------

class DB:
    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self.conn = self._connect()
        self._init_global_tables()

    def _connect(self):
    return mysql.connector.connect(
        host=self.cfg.host,
        user=self.cfg.user,
        password=self.cfg.password,
        database=self.cfg.database,
        port=self.cfg.port,
    )

    def _cursor(self, dictionary=False):
        try:
            if not self.conn.is_connected():
                self.conn.reconnect()
        except Exception:
            self.conn = self._connect()
        return self.conn.cursor(dictionary=dictionary)

    def _init_global_tables(self):
        cur = self._cursor()
        # Master table for source posts
        cur.execute("""
        CREATE TABLE IF NOT EXISTS source_posts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            guid VARCHAR(255) NOT NULL UNIQUE,
            url TEXT,
            title TEXT,
            published_at DATETIME,
            scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        # Optional global log
        cur.execute("""
        CREATE TABLE IF NOT EXISTS post_push_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            guid VARCHAR(255),
            target_name VARCHAR(255),
            wp_post_id INT,
            status_code INT,
            success TINYINT(1),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        self.conn.commit()

    @staticmethod
    def sanitize_table_name(domain: str) -> str:
        # domain -> safe table name like wp_example_com_posts
        dom = domain.lower()
        dom = re.sub(r'[^a-z0-9]+', '_', dom)
        dom = dom.strip('_')
        return f"wp_{dom}_posts"

    def ensure_site_table(self, domain: str) -> str:
        table_name = self.sanitize_table_name(domain)
        cur = self._cursor()
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            guid VARCHAR(255) NOT NULL,
            wp_post_id INT NOT NULL,
            posted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_guid (guid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        self.conn.commit()
        return table_name

    # Source-level dedup
    def has_source_post(self, guid: str) -> bool:
        cur = self._cursor()
        cur.execute("SELECT 1 FROM source_posts WHERE guid=%s", (guid,))
        return cur.fetchone() is not None

    def insert_source_post(self, item: PostItem):
        cur = self._cursor()
        cur.execute("""
            INSERT IGNORE INTO source_posts (guid, url, title, published_at)
            VALUES (%s, %s, %s, %s)
        """, (
            item.guid,
            item.url,
            item.title,
            item.published_at.strftime("%Y-%m-%d %H:%M:%S") if item.published_at else None
        ))
        self.conn.commit()

    # Per-site tracking
    def site_post_exists(self, table_name: str, guid: str) -> bool:
        cur = self._cursor()
        cur.execute(f"SELECT 1 FROM `{table_name}` WHERE guid=%s", (guid,))
        return cur.fetchone() is not None

    def mark_site_post(self, table_name: str, guid: str, wp_post_id: int):
        cur = self._cursor()
        cur.execute(
            f"INSERT IGNORE INTO `{table_name}` (guid, wp_post_id) VALUES (%s, %s)",
            (guid, wp_post_id)
        )
        self.conn.commit()

    def log_push(self, guid: str, target_name: str, wp_post_id: Optional[int],
                 status_code: Optional[int], success: bool):
        cur = self._cursor()
        cur.execute("""
            INSERT INTO post_push_log (guid, target_name, wp_post_id, status_code, success)
            VALUES (%s, %s, %s, %s, %s)
        """, (guid, target_name, wp_post_id, status_code, 1 if success else 0))
        self.conn.commit()


# ------------- SOURCE FETCHER (RSS + FULL ARTICLE) -------------

class SourceFetcher:
    def __init__(self, cfg: SourceConfig, runtime: RuntimeConfig):
        self.cfg = cfg
        self.runtime = runtime
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": runtime.user_agent})
        try:
            self.tz = pytz.timezone(cfg.timezone)
        except Exception:
            self.tz = pytz.UTC

    def fetch_new(self, db: DB) -> List[PostItem]:
        if self.cfg.mode.lower() != "rss":
            logging.error("Only RSS mode supported for now.")
            return []

        logging.info(f"Fetching RSS: {self.cfg.rss_url}")
        try:
            resp = self.session.get(self.cfg.rss_url, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            logging.error(f"RSS fetch error: {e}")
            return []

        soup = BeautifulSoup(resp.text, "xml")
        items = soup.find_all("item")
        new_posts: List[PostItem] = []

        for it in items:
            guid_tag = it.find("guid")
            link_tag = it.find("link")
            title_tag = it.find("title")
            date_tag = it.find("pubDate")

            guid = guid_tag.text.strip() if guid_tag else (link_tag.text.strip() if link_tag else None)
            url = link_tag.text.strip() if link_tag else None
            title = title_tag.text.strip() if title_tag else "(No title)"

            if not guid or not url:
                continue

            if db.has_source_post(guid):
                continue

            published_at = None
            if date_tag and date_tag.text:
                try:
                    published_at = dateparser.parse(date_tag.text)
                    if published_at.tzinfo is None:
                        published_at = self.tz.localize(published_at)
                except Exception:
                    published_at = None

            # Prefer full content from article page
            if self.cfg.full_content_from_article_page:
                content_html = self._fetch_article_content(url)
            else:
                desc_tag = it.find("description")
                content_html = desc_tag.text if desc_tag else ""

            item = PostItem(
                guid=guid,
                url=url,
                title=title,
                content_html=content_html,
                published_at=published_at
            )
            new_posts.append(item)

        logging.info(f"New posts from source: {len(new_posts)}")
        return new_posts

    def _fetch_article_content(self, url: str) -> str:
        logging.info(f"Fetching full article: {url}")
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logging.error(f"Article fetch error: {e}")
            return ""

        soup = BeautifulSoup(resp.text, "html.parser")
        node = soup.select_one(self.cfg.article_content_selector)
        if not node:
            logging.warning(f"Content selector not found for: {url}")
            return ""

        # Fix relative URLs (links + images)
        for tag in node.find_all(["a", "img", "source"]):
            if tag.name == "a" and tag.has_attr("href"):
                tag["href"] = urljoin(url, tag["href"])
            if tag.name in ("img", "source") and tag.has_attr("src"):
                tag["src"] = urljoin(url, tag["src"])

        return str(node)


# ------------- WORDPRESS CLIENT (REST + MEDIA) -------------

class WordPressClient:
    def __init__(self, cfg: TargetConfig, runtime: RuntimeConfig):
        self.cfg = cfg
        self.runtime = runtime
        self.session = requests.Session()
        self.session.auth = (cfg.username, cfg.application_password)
        self.session.headers.update({"User-Agent": runtime.user_agent})

        parsed = urlparse(self.cfg.base_url)
        self.domain = parsed.netloc.split(":")[0]

    def _posts_endpoint(self) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/wp-json/wp/v2/{self.cfg.post_type}"

    def _media_endpoint(self) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/wp-json/wp/v2/media"

    def _guess_mime_from_url(self, url: str) -> str:
        mime, _ = mimetypes.guess_type(url)
        if not mime:
            return "image/jpeg"
        return mime

    def _upload_and_rehost_images(self, html: str) -> (str, Optional[int]):
        """
        Download all <img> src from source, upload to this WP site,
        replace src in HTML with new URLs.
        Return (new_html, first_media_id_to_use_as_featured).
        """
        if not html:
            return html, None

        soup = BeautifulSoup(html, "html.parser")
        imgs = soup.find_all("img")
        if not imgs:
            return html, None

        featured_media_id = None

        for idx, img in enumerate(imgs):
            src = img.get("src")
            if not src:
                continue

            try:
                logging.info(f"[{self.cfg.name}] Downloading image: {src}")
                img_resp = requests.get(src, timeout=20)
                img_resp.raise_for_status()
                img_bytes = img_resp.content
            except Exception as e:
                logging.warning(f"[{self.cfg.name}] Image download failed ({src}): {e}")
                continue

            filename = src.split("/")[-1].split("?")[0] or f"image_{idx}.jpg"
            mime_type = self._guess_mime_from_url(src)

            files = {
                'file': (filename, img_bytes, mime_type)
            }
            headers = {
                "Content-Disposition": f'attachment; filename="{filename}"'
            }

            try:
                logging.info(f"[{self.cfg.name}] Uploading image to media library: {filename}")
                r = self.session.post(
                    self._media_endpoint(),
                    files=files,
                    headers=headers,
                    timeout=30
                )
            except Exception as e:
                logging.error(f"[{self.cfg.name}] Media upload error: {e}")
                continue

            if r.status_code not in (200, 201):
                logging.error(f"[{self.cfg.name}] Media upload failed ({r.status_code}): {r.text[:200]}")
                continue

            try:
                data = r.json()
            except Exception:
                logging.error(f"[{self.cfg.name}] Invalid JSON in media response")
                continue

            new_url = data.get("source_url")
            media_id = data.get("id")
            if not new_url or not media_id:
                logging.error(f"[{self.cfg.name}] Missing media data in response")
                continue

            img["src"] = new_url
            if featured_media_id is None:
                featured_media_id = media_id

        return str(soup), featured_media_id

    def create_post(self, item: PostItem) -> (bool, Optional[int], Optional[int]):
        # Rehost all images and get featured image media id
        processed_html, featured_media_id = self._upload_and_rehost_images(item.content_html)

        payload: Dict[str, Any] = {
            "title": item.title,
            "content": processed_html,
            "status": self.cfg.default_status
        }

        if item.published_at is not None:
            payload["date"] = item.published_at.isoformat()

        if self.cfg.default_categories:
            payload["categories"] = self.cfg.default_categories
        if self.cfg.default_tags:
            payload["tags"] = self.cfg.default_tags

        if featured_media_id:
            payload["featured_media"] = featured_media_id

        # Optional: slug from source URL
        try:
            slug = urlparse(item.url).path.strip("/").split("/")[-1]
            if slug:
                payload["slug"] = slug
        except Exception:
            pass

        api_url = self._posts_endpoint()
        logging.info(f"[{self.cfg.name}] Creating post: {item.title!r}")

        try:
            r = self.session.post(api_url, json=payload, timeout=40)
        except Exception as e:
            logging.error(f"[{self.cfg.name}] Post create error: {e}")
            return False, None, None

        if r.status_code in (200, 201):
            try:
                data = r.json()
            except Exception:
                data = {}
            wp_id = data.get("id")
            logging.info(f"[{self.cfg.name}] Created post ID {wp_id}")
            return True, wp_id, r.status_code

        logging.error(f"[{self.cfg.name}] Post create failed {r.status_code}: {r.text[:400]}")
        return False, None, r.status_code


# ------------- MAIN CONTROLLER -------------

class AutoPoster:
    def __init__(self, config_path: str = "config.json"):
        with open(config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        db_cfg = DBConfig(**raw["db"])
        self.db = DB(db_cfg)

        self.source_cfg = SourceConfig(**raw["source"])
        self.targets_cfg = [TargetConfig(**t) for t in raw["targets"]]
        self.runtime_cfg = RuntimeConfig(**raw["runtime"])

        logging.basicConfig(
            level=getattr(logging, self.runtime_cfg.log_level.upper(), logging.INFO),
            format="[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        self.fetcher = SourceFetcher(self.source_cfg, self.runtime_cfg)
        self.clients: List[WordPressClient] = []
        self.site_tables: Dict[str, str] = {}

        # Initialize WP clients and per-site tables
        for t in self.targets_cfg:
            client = WordPressClient(t, self.runtime_cfg)
            self.clients.append(client)
            table_name = self.db.ensure_site_table(client.domain)
            self.site_tables[client.domain] = table_name
            logging.info(f"Site '{t.name}' domain '{client.domain}' uses table '{table_name}'")

    def run_forever(self):
        logging.info("=== WP Auto Poster (MySQL + Rehost Images) Started ===")
        while True:
            try:
                self.single_cycle()
            except KeyboardInterrupt:
                logging.info("Interrupted by user, exiting.")
                break
            except Exception as e:
                logging.error(f"Fatal error in main loop: {e}")
                traceback.print_exc()

            logging.info(f"Sleeping {self.runtime_cfg.poll_interval_seconds} seconds...")
            time.sleep(self.runtime_cfg.poll_interval_seconds)

    def single_cycle(self):
        logging.info("Starting new cycle...")
        new_items = self.fetcher.fetch_new(self.db)
        if not new_items:
            logging.info("No new posts in source feed.")
            return

        # Limit per cycle
        new_items = new_items[: self.runtime_cfg.max_posts_per_cycle]

        for item in new_items:
            logging.info(f"Processing GUID={item.guid} TITLE={item.title!r}")

            # Insert into source_posts (mark that we have seen this guid)
            self.db.insert_source_post(item)

            for client in self.clients:
                table_name = self.site_tables[client.domain]

                # If already posted to this specific site, skip
                if self.db.site_post_exists(table_name, item.guid):
                    logging.info(f"[{client.cfg.name}] GUID already posted to this site, skipping.")
                    continue

                success, wp_id, status_code = client.create_post(item)
                self.db.log_push(item.guid, client.cfg.name, wp_id, status_code, success)

                if success and wp_id:
                    self.db.mark_site_post(table_name, item.guid, wp_id)


if __name__ == "__main__":
    poster = AutoPoster("config.json")
    poster.run_forever()
