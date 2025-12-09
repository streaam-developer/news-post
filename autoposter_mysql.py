#!/usr/bin/env python3
import json
import logging
import time
import traceback
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse
import mimetypes

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import pytz
import mysql.connector


# ---------- GLOBAL LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.info("=== AutoPoster script starting ===")


# ---------- CLEAN ARTICLE EXTRACTOR ----------

def clean_article_content(page_html: str, base_url: str, selector: Optional[str] = None) -> str:
    """
    Page HTML se sirf clean article content nikale:
    - h2, p, ul/li, img
    - Ads / Taboola / storyAd / iframes / scripts hata de
    - Classes, inline styles ignore (hum khud simple HTML banayenge)
    """
    if not page_html:
        return ""

    # Try lxml, fallback html.parser
    try:
        soup = BeautifulSoup(page_html, "lxml")
    except Exception:
        soup = BeautifulSoup(page_html, "html.parser")

    container = None

    # 1) Config selector
    if selector:
        container = soup.select_one(selector)

    # 2) HindustanTimes ka known wrapper
    if container is None:
        container = soup.find("div", class_="articleDetail_artContent__F_8dX")

    # 3) Fallback: body
    if container is None:
        container = soup.body or soup

    # Ads / taboola / poll blocks remove
    for bad in container.select(
        ".storyAd, .trc_related_container, .taboola-readmore, "
        ".pollDivElement, #taboola-article-recommendation, #taboola-story-101765264870828"
    ):
        bad.decompose()

    # Scripts, styles, iframes remove
    for tag in container.find_all(["script", "style", "iframe", "noscript"]):
        tag.decompose()

    # Fix relative URLs
    for tag in container.find_all(["a", "img", "source"]):
        if tag.name == "a" and tag.has_attr("href"):
            tag["href"] = urljoin(base_url, tag["href"])
        if tag.name in ("img", "source") and tag.has_attr("src"):
            tag["src"] = urljoin(base_url, tag["src"])

    clean_parts: List[str] = []

    for node in container.descendants:
        if not hasattr(node, "name"):
            continue

        if node.name == "h2":
            text = node.get_text(strip=True)
            if text:
                clean_parts.append(f"<h2>{text}</h2>")

        elif node.name == "p":
            text = node.get_text(strip=True)
            if text:
                clean_parts.append(f"<p>{text}</p>")

        elif node.name == "ul":
            lis = []
            for li in node.find_all("li", recursive=False):
                t = li.get_text(strip=True)
                if t:
                    lis.append(f"<li>{t}</li>")
            if lis:
                clean_parts.append(f"<ul>{''.join(lis)}</ul>")

        elif node.name == "img":
            src = node.get("src") or node.get("data-src")
            if src:
                clean_parts.append(f'<p><img src="{src}"></p>')

    html = "\n".join(clean_parts)
    logging.info(f"[CLEAN] Final clean content length={len(html)}")
    return html


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
    rss_categories: List[str] = field(default_factory=list)


# ------------- DB LAYER (MySQL) -------------

class DB:
    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self.conn = self._connect()
        self._init_global_tables()

    def _connect(self):
        logging.info(f"[DB] Connecting to MySQL {self.cfg.host}:{self.cfg.port} db={self.cfg.database}")
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
                logging.warning("[DB] Connection lost, reconnecting...")
                self.conn.reconnect()
        except Exception:
            self.conn = self._connect()
        return self.conn.cursor(dictionary=dictionary)

    def _init_global_tables(self):
        cur = self._cursor()
        logging.info("[DB] Ensuring source_posts & post_push_log tables exist...")
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
        dom = domain.lower()
        dom = re.sub(r'[^a-z0-9]+', '_', dom)
        dom = dom.strip('_')
        return f"wp_{dom}_posts"

    def ensure_site_table(self, domain: str) -> str:
        table_name = self.sanitize_table_name(domain)
        cur = self._cursor()
        logging.info(f"[DB] Ensuring table for site {domain}: {table_name}")
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
        logging.info(f"[DB] Inserting source post GUID={item.guid}")
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
        logging.info(f"[DB] Marking GUID={guid} posted in {table_name} with WP ID={wp_post_id}")
        cur.execute(
            f"INSERT IGNORE INTO `{table_name}` (guid, wp_post_id) VALUES (%s, %s)",
            (guid, wp_post_id)
        )
        self.conn.commit()

    def log_push(self, guid: str, target_name: str, wp_post_id: Optional[int],
                 status_code: Optional[int], success: bool):
        cur = self._cursor()
        logging.info(f"[DB] Logging push guid={guid} target={target_name} success={success} status={status_code}")
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

        logging.info(f"[SRC] Fetching RSS: {self.cfg.rss_url}")
        try:
            resp = self.session.get(self.cfg.rss_url, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            logging.error(f"[SRC] RSS fetch error: {e}")
            return []

        logging.info(f"[SRC] RSS HTTP status={resp.status_code}, len={len(resp.text)}")

        # Parse as XML if possible, fallback to html.parser
        try:
            soup = BeautifulSoup(resp.text, "xml")
        except Exception as e:
            logging.warning(f"[SRC] XML parser failed ({e}), falling back to html.parser")
            soup = BeautifulSoup(resp.text, "html.parser")

        items = soup.find_all("item")
        logging.info(f"[SRC] RSS items found: {len(items)}")

        new_posts: List[PostItem] = []

        for idx, it in enumerate(items):
            guid_tag = it.find("guid")
            link_tag = it.find("link")
            title_tag = it.find("title")
            date_tag = it.find("pubDate")

            # GUID
            guid = None
            if guid_tag:
                txt = (guid_tag.text or "").strip()
                if txt:
                    guid = txt

            # URL
            url = None
            if link_tag:
                txt = (link_tag.text or "").strip()
                if txt:
                    url = txt
                elif link_tag.has_attr("href"):
                    url = link_tag["href"].strip()

            # Fallback: guid = url if guid missing
            if not guid and url:
                guid = url

            title = title_tag.text.strip() if title_tag and title_tag.text else "(No title)"

            # RSS categories (e.g. <category>business</category>)
            rss_cats = []
            for c in it.find_all("category"):
                if c.text and c.text.strip():
                    rss_cats.append(c.text.strip())

            logging.info(f"[SRC] Item #{idx} GUID={guid} URL={url} TITLE={title} CATS={rss_cats}")

            if not guid or not url:
                logging.warning(f"[SRC] Skipping item #{idx} because GUID or URL missing")
                continue

            if db.has_source_post(guid):
                logging.info(f"[SRC] GUID={guid} already in source_posts, skipping")
                continue

            published_at = None
            if date_tag and date_tag.text:
                try:
                    published_at = dateparser.parse(date_tag.text)
                    if published_at.tzinfo is None:
                        published_at = self.tz.localize(published_at)
                except Exception as e:
                    logging.warning(f"[SRC] Failed to parse date for GUID={guid}: {e}")
                    published_at = None

            content_html = ""
            if self.cfg.full_content_from_article_page:
                content_html = self._fetch_article_content(url, guid)
            else:
                desc_tag = it.find("description")
                content_html = desc_tag.text if desc_tag and desc_tag.text else ""

            logging.info(f"[SRC] GUID={guid} content length after extract: {len(content_html)}")

            item = PostItem(
                guid=guid,
                url=url,
                title=title,
                content_html=content_html,
                published_at=published_at,
                rss_categories=rss_cats,
            )
            new_posts.append(item)

        logging.info(f"[SRC] New posts collected this cycle: {len(new_posts)}")
        return new_posts

    def _fetch_article_content(self, url: str, guid: str) -> str:
        logging.info(f"[SRC] Fetching full article for GUID={guid}, URL={url}")
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logging.error(f"[SRC] Article fetch error for GUID={guid}, URL={url}: {e}")
            return ""

        logging.info(f"[SRC] Article HTTP status={resp.status_code}, length={len(resp.text)}")

        html = clean_article_content(resp.text, url, selector=self.cfg.article_content_selector)
        logging.info(f"[SRC] Extracted HTML length for GUID={guid}: {len(html)}")
        return html


# ------------- WORDPRESS CLIENT (REST + MEDIA + CATEGORIES) -------------

class WordPressClient:
    def __init__(self, cfg: TargetConfig, runtime: RuntimeConfig):
        self.cfg = cfg
        self.runtime = runtime
        self.session = requests.Session()
        self.session.auth = (cfg.username, cfg.application_password)
        self.session.headers.update({"User-Agent": runtime.user_agent})

        parsed = urlparse(self.cfg.base_url)
        self.domain = parsed.netloc.split(":")[0]

        # cache: category_name_lower -> id
        self._category_cache: Dict[str, int] = {}

    def _posts_endpoint(self) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/wp-json/wp/v2/{self.cfg.post_type}"

    def _media_endpoint(self) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/wp-json/wp/v2/media"

    def _categories_endpoint(self) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/wp-json/wp/v2/categories"

    @staticmethod
    def _guess_mime_from_url(url: str) -> str:
        mime, _ = mimetypes.guess_type(url)
        if not mime:
            return "image/jpeg"
        return mime

    @staticmethod
    def _slugify(name: str) -> str:
        s = name.strip().lower()
        s = re.sub(r'[^a-z0-9]+', '-', s)
        s = s.strip('-')
        return s or "category"

    def _ensure_categories(self, names: List[str]) -> List[int]:
        """RSS category names ko WP categories me map kare (auto-create if needed)"""
        ids: List[int] = []
        for name in names:
            if not name:
                continue
            key = name.strip().lower()
            if key in self._category_cache:
                ids.append(self._category_cache[key])
                continue

            slug = self._slugify(name)
            # 1) Try to fetch existing category by slug
            try:
                r = self.session.get(self._categories_endpoint(), params={"slug": slug}, timeout=20)
                if r.status_code == 200:
                    arr = r.json()
                    if isinstance(arr, list) and arr:
                        cat_id = arr[0].get("id")
                        if cat_id:
                            self._category_cache[key] = cat_id
                            ids.append(cat_id)
                            logging.info(f"[{self.cfg.name}] Using existing category '{name}' (id={cat_id})")
                            continue
            except Exception as e:
                logging.warning(f"[{self.cfg.name}] Category GET error for '{name}': {e}")

            # 2) Create new category
            try:
                payload = {"name": name, "slug": slug}
                r = self.session.post(self._categories_endpoint(), json=payload, timeout=20)
                if r.status_code in (200, 201):
                    data = r.json()
                    cat_id = data.get("id")
                    if cat_id:
                        self._category_cache[key] = cat_id
                        ids.append(cat_id)
                        logging.info(f"[{self.cfg.name}] Created category '{name}' (id={cat_id})")
                else:
                    logging.error(
                        f"[{self.cfg.name}] Failed to create category '{name}' "
                        f"status={r.status_code} resp={r.text[:200]}"
                    )
            except Exception as e:
                logging.error(f"[{self.cfg.name}] Category POST error for '{name}': {e}")

        return ids

    def _upload_and_rehost_images(self, html: str, guid: str, source_url: str) -> (str, Optional[int]):
        if not html:
            logging.info(f"[{self.cfg.name}] Empty HTML for GUID={guid}, skipping image rehost.")
            return html, None

        soup = BeautifulSoup(html, "html.parser")
        imgs = soup.find_all("img")
        logging.info(f"[{self.cfg.name}] GUID={guid} images found in content: {len(imgs)}")
        if not imgs:
            return html, None

        featured_media_id = None

        for idx, img in enumerate(imgs):
            src = img.get("src")
            if not src:
                continue

            try:
                logging.info(f"[{self.cfg.name}] GUID={guid} downloading image #{idx}: {src}")
                # 👉 HOTLINK FIX: send Referer (original article URL)
                r_img = self.session.get(
                    src,
                    timeout=20,
                    headers={"Referer": source_url}
                )
                r_img.raise_for_status()
                img_bytes = r_img.content
            except Exception as e:
                logging.warning(
                    f"[{self.cfg.name}] GUID={guid} image download failed ({src}): {e}"
                )
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
                logging.info(f"[{self.cfg.name}] GUID={guid} uploading image to media: {filename}")
                r = self.session.post(
                    self._media_endpoint(),
                    files=files,
                    headers=headers,
                    timeout=40
                )
            except Exception as e:
                logging.error(f"[{self.cfg.name}] GUID={guid} media upload error: {e}")
                continue

            if r.status_code not in (200, 201):
                logging.error(f"[{self.cfg.name}] GUID={guid} media upload failed {r.status_code}: {r.text[:300]}")
                continue

            try:
                data = r.json()
            except Exception:
                logging.error(f"[{self.cfg.name}] GUID={guid} invalid JSON in media response")
                continue

            new_url = data.get("source_url")
            media_id = data.get("id")
            logging.info(f"[{self.cfg.name}] GUID={guid} media uploaded, id={media_id}, new_url={new_url}")

            if not new_url or not media_id:
                logging.error(f"[{self.cfg.name}] GUID={guid} missing media data in response")
                continue

            img["src"] = new_url
            if featured_media_id is None:
                featured_media_id = media_id

        final_html = str(soup)
        logging.info(f"[{self.cfg.name}] GUID={guid} final HTML length after rehost: {len(final_html)}")
        return final_html, featured_media_id

    def create_post(self, item: PostItem) -> (bool, Optional[int], Optional[int]):
        logging.info(f"[{self.cfg.name}] Preparing post for GUID={item.guid}, TITLE={item.title!r}")

        # RSS categories -> WP categories ids
        rss_cat_ids = self._ensure_categories(item.rss_categories)
        # merge with default categories (and de-duplicate)
        all_cat_ids = list({*(rss_cat_ids or []), *(self.cfg.default_categories or [])})

        processed_html, featured_media_id = self._upload_and_rehost_images(
            item.content_html,
            item.guid,
            item.url
        )

        payload: Dict[str, Any] = {
            "title": item.title,
            "content": processed_html,
            "status": self.cfg.default_status
        }

        if item.published_at is not None:
            payload["date"] = item.published_at.isoformat()

        if all_cat_ids:
            payload["categories"] = all_cat_ids

        if self.cfg.default_tags:
            payload["tags"] = self.cfg.default_tags

        if featured_media_id:
            payload["featured_media"] = featured_media_id

        try:
            slug = urlparse(item.url).path.strip("/").split("/")[-1]
            if slug:
                payload["slug"] = slug
        except Exception:
            pass

        api_url = self._posts_endpoint()
        logging.info(
            f"[{self.cfg.name}] Creating post via {api_url} | GUID={item.guid} | "
            f"content_len={len(processed_html)} cats={all_cat_ids}"
        )

        try:
            r = self.session.post(api_url, json=payload, timeout=60)
        except Exception as e:
            logging.error(f"[{self.cfg.name}] GUID={item.guid} post create error: {e}")
            return False, None, None

        if r.status_code in (200, 201):
            try:
                data = r.json()
            except Exception:
                data = {}
            wp_id = data.get("id")
            logging.info(f"[{self.cfg.name}] GUID={item.guid} post created ID={wp_id}")
            return True, wp_id, r.status_code

        logging.error(
            f"[{self.cfg.name}] GUID={item.guid} post create failed {r.status_code}. "
            f"Response: {r.text[:500]}"
        )
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

        logging.getLogger().setLevel(
            getattr(logging, self.runtime_cfg.log_level.upper(), logging.INFO)
        )

        self.fetcher = SourceFetcher(self.source_cfg, self.runtime_cfg)
        self.clients: List[WordPressClient] = []
        self.site_tables: Dict[str, str] = {}

        for t in self.targets_cfg:
            client = WordPressClient(t, self.runtime_cfg)
            self.clients.append(client)
            table_name = self.db.ensure_site_table(client.domain)
            self.site_tables[client.domain] = table_name
            logging.info(f"[INIT] Site '{t.name}' domain '{client.domain}' uses table '{table_name}'")

    def run_forever(self):
        logging.info("=== WP Auto Poster (MySQL + Clean Content + Rehost Images + Categories) Started ===")
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
        logging.info("----- NEW CYCLE -----")
        new_items = self.fetcher.fetch_new(self.db)
        if not new_items:
            logging.info("[CYCLE] No new posts in source feed.")
            return

        logging.info(f"[CYCLE] Items to process (after source dedup): {len(new_items)}")
        new_items = new_items[: self.runtime_cfg.max_posts_per_cycle]

        for item in new_items:
            logging.info(f"[CYCLE] Processing GUID={item.guid}, TITLE={item.title!r}")
            self.db.insert_source_post(item)

            for client in self.clients:
                table_name = self.site_tables[client.domain]

                if self.db.site_post_exists(table_name, item.guid):
                    logging.info(f"[{client.cfg.name}] GUID={item.guid} already posted to this site, skipping.")
                    continue

                success, wp_id, status_code = client.create_post(item)
                self.db.log_push(item.guid, client.cfg.name, wp_id, status_code, success)

                if success and wp_id:
                    self.db.mark_site_post(table_name, item.guid, wp_post_id=wp_id)


if __name__ == "__main__":
    poster = AutoPoster("config.json")
    poster.run_forever()
