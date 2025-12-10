#!/usr/bin/env python3
"""
Autoposter (modified/fixed):
- More verbose logging (DEBUG by default if configured)
- Requests sessions with retries & backoff
- CLI flags: --config, --once, --dry-run, --limit-feeds
- Optional test_rss_only in config to restrict feeds during debugging
- Improved DB reconnect handling and clearer WP error logging
- Keeps original structure but with safer defaults for debugging
"""

import argparse
import json
import logging
import time
import traceback
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import urljoin, urlparse
import mimetypes
import os
import sys

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import pytz
import mysql.connector

# ------------- Helpful Constants -------------
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

# ---------- LOGGING - will be set from config at runtime -----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ----------------- UTIL: requests session with retries -----------------
def build_session(user_agent: str, max_retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ----------------- CLEAN ARTICLE EXTRACTOR (kept from original) -----------------
def clean_article_content(page_html: str, base_url: str, selector: Optional[str] = None) -> str:
    if not page_html:
        return ""

    try:
        soup = BeautifulSoup(page_html, "lxml")
    except Exception:
        soup = BeautifulSoup(page_html, "html.parser")

    container = None

    if selector:
        try:
            container = soup.select_one(selector)
        except Exception:
            container = None

    if container is None:
        container = soup.find("div", class_="articleDetail_artContent__F_8dX")

    if container is None:
        container = soup.body or soup

    for bad in container.select(
        ".storyAd, .trc_related_container, .taboola-readmore, "
        ".pollDivElement, #taboola-article-recommendation, #taboola-story-101765264870828"
    ):
        bad.decompose()

    for tag in container.find_all(["script", "style", "iframe", "noscript"]):
        tag.decompose()

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
    logging.debug(f"[CLEAN] Final clean content length={len(html)}")
    return html


# ----------------- DATA CLASSES -----------------
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
    rss_url: List[str]
    full_content_from_article_page: bool
    article_content_selector: Optional[str]
    title_selector: Optional[str]
    timezone: str
    # optional debug/testing helpers
    test_rss_only: Optional[bool] = False
    test_rss_list: Optional[List[str]] = None


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


# ----------------- DB LAYER -----------------
class DB:
    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self.conn = None
        self._connect_with_retry()
        self._init_global_tables()

    def _connect_with_retry(self, tries: int = 3, delay: float = 2.0):
        last_exc = None
        for attempt in range(1, tries + 1):
            try:
                logging.info(f"[DB] Connecting to MySQL {self.cfg.host}:{self.cfg.port} db={self.cfg.database} (attempt {attempt})")
                self.conn = mysql.connector.connect(
                    host=self.cfg.host,
                    user=self.cfg.user,
                    password=self.cfg.password,
                    database=self.cfg.database,
                    port=self.cfg.port,
                    autocommit=False,
                    connection_timeout=10
                )
                logging.debug("[DB] Connected successfully.")
                return
            except Exception as e:
                last_exc = e
                logging.error(f"[DB] Connect attempt {attempt} failed: {e}")
                time.sleep(delay)
        logging.critical("[DB] All connection attempts failed. Raising exception.")
        raise last_exc

    def _cursor(self, dictionary: bool = False):
        try:
            if self.conn is None or not self.conn.is_connected():
                logging.warning("[DB] Connection lost or closed; reconnecting...")
                self._connect_with_retry()
        except Exception as e:
            logging.error(f"[DB] Reconnect failed: {e}")
            self._connect_with_retry()
        return self.conn.cursor(dictionary=dictionary)

    def _init_global_tables(self):
        cur = self._cursor()
        logging.debug("[DB] Ensuring global tables exist...")
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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS domain_rss_mapping (
            id INT AUTO_INCREMENT PRIMARY KEY,
            domain VARCHAR(255) NOT NULL UNIQUE,
            rss_url TEXT NOT NULL
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
        logging.debug(f"[DB] Ensuring table for site {domain}: {table_name}")
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

    def has_source_post(self, guid: str) -> bool:
        cur = self._cursor()
        cur.execute("SELECT 1 FROM source_posts WHERE guid=%s", (guid,))
        return cur.fetchone() is not None

    def insert_source_post(self, item: PostItem):
        cur = self._cursor()
        logging.debug(f"[DB] Inserting source post GUID={item.guid}")
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

    def get_rss_for_domain(self, domain: str) -> Optional[str]:
        cur = self._cursor(dictionary=True)
        cur.execute("SELECT rss_url FROM domain_rss_mapping WHERE domain=%s", (domain,))
        row = cur.fetchone()
        return row['rss_url'] if row else None

    def set_rss_for_domain(self, domain: str, rss_url: str):
        cur = self._cursor()
        cur.execute("INSERT INTO domain_rss_mapping (domain, rss_url) VALUES (%s, %s) ON DUPLICATE KEY UPDATE rss_url=%s", (domain, rss_url, rss_url))
        self.conn.commit()

    def site_post_exists(self, table_name: str, guid: str) -> bool:
        cur = self._cursor()
        cur.execute(f"SELECT 1 FROM `{table_name}` WHERE guid=%s", (guid,))
        return cur.fetchone() is not None

    def mark_site_post(self, table_name: str, guid: str, wp_post_id: int):
        cur = self._cursor()
        logging.debug(f"[DB] Marking GUID={guid} posted in {table_name} with WP ID={wp_post_id}")
        cur.execute(
            f"INSERT IGNORE INTO `{table_name}` (guid, wp_post_id) VALUES (%s, %s)",
            (guid, wp_post_id)
        )
        self.conn.commit()

    def log_push(self, guid: str, target_name: str, wp_post_id: Optional[int],
                 status_code: Optional[int], success: bool):
        cur = self._cursor()
        logging.debug(f"[DB] Logging push guid={guid} target={target_name} success={success} status={status_code}")
        cur.execute("""
            INSERT INTO post_push_log (guid, target_name, wp_post_id, status_code, success)
            VALUES (%s, %s, %s, %s, %s)
        """, (guid, target_name, wp_post_id, status_code, 1 if success else 0))
        self.conn.commit()


# ----------------- SOURCE FETCHER -----------------
class SourceFetcher:
    def __init__(self, cfg: SourceConfig, runtime: RuntimeConfig):
        self.cfg = cfg
        self.runtime = runtime
        ua = runtime.user_agent or DEFAULT_USER_AGENT
        self.session = build_session(ua)
        try:
            self.tz = pytz.timezone(cfg.timezone)
        except Exception:
            self.tz = pytz.UTC

    def fetch_new(self, db: DB, limit_feeds: Optional[int] = None) -> List[PostItem]:
        if self.cfg.mode.lower() != "rss":
            logging.error("Only RSS mode supported for now.")
            return []

        rss_list = list(self.cfg.rss_url or [])
        if self.cfg.test_rss_only and self.cfg.test_rss_list:
            logging.info("[SRC] test_rss_only enabled — using test_rss_list from config")
            rss_list = list(self.cfg.test_rss_list)

        if limit_feeds:
            rss_list = rss_list[:limit_feeds]

        new_posts: List[PostItem] = []

        for rss_url in rss_list:
            logging.debug(f"[SRC] Fetching RSS: {rss_url}")
            try:
                resp = self.session.get(rss_url, timeout=20)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[SRC] RSS fetch error for {rss_url}: {e} (status: {getattr(e.response, 'status_code', 'N/A')})")
                # Dump short response body if exists
                if hasattr(e, "response") and e.response is not None:
                    try:
                        logging.debug(f"[SRC] RSS response body (first 500 chars): {e.response.text[:500]}")
                    except Exception:
                        pass
                continue

            logging.debug(f"[SRC] RSS HTTP status={resp.status_code}, len={len(resp.text)}")

            try:
                soup = BeautifulSoup(resp.text, "xml")
            except Exception as e:
                logging.warning(f"[SRC] XML parser failed for {rss_url} ({e}), falling back to html.parser")
                soup = BeautifulSoup(resp.text, "html.parser")

            items = soup.find_all("item")
            logging.info(f"[SRC] RSS items found in {rss_url}: {len(items)}")

            for idx, it in enumerate(items):
                guid_tag = it.find("guid")
                link_tag = it.find("link")
                title_tag = it.find("title")
                date_tag = it.find("pubDate")

                guid = None
                if guid_tag:
                    txt = (guid_tag.text or "").strip()
                    if txt:
                        guid = txt

                url = None
                if link_tag:
                    txt = (link_tag.text or "").strip()
                    if txt:
                        url = txt
                    elif link_tag.has_attr("href"):
                        url = link_tag["href"].strip()

                if not guid and url:
                    guid = url

                title = title_tag.text.strip() if title_tag and title_tag.text else "(No title)"

                rss_cats: List[str] = []
                for c in it.find_all("category"):
                    if c.text and c.text.strip():
                        rss_cats.append(c.text.strip())

                logging.debug(f"[SRC] Item #{idx} GUID={guid} URL={url} TITLE={title} CATS={rss_cats}")

                if not guid or not url:
                    logging.warning(f"[SRC] Skipping item #{idx} because GUID or URL missing")
                    continue

                if db.has_source_post(guid):
                    logging.debug(f"[SRC] GUID={guid} already in source_posts, skipping")
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

                logging.debug(f"[SRC] GUID={guid} content length after extract: {len(content_html)}")

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
        logging.debug(f"[SRC] Fetching full article for GUID={guid}, URL={url}")
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"[SRC] Article fetch error for GUID={guid}, URL={url}: {e}")
            if hasattr(e, "response") and e.response is not None:
                try:
                    logging.debug(f"[SRC] Article response body (first 1000 chars): {e.response.text[:1000]}")
                except Exception:
                    pass
            return ""

        logging.debug(f"[SRC] Article HTTP status={resp.status_code}, length={len(resp.text)}")
        html = clean_article_content(resp.text, url, selector=self.cfg.article_content_selector)
        logging.debug(f"[SRC] Extracted HTML length for GUID={guid}: {len(html)}")
        return html


# ----------------- WORDPRESS CLIENT -----------------
class WordPressClient:
    def __init__(self, cfg: TargetConfig, runtime: RuntimeConfig):
        self.cfg = cfg
        self.runtime = runtime
        ua = runtime.user_agent or DEFAULT_USER_AGENT
        self.session = build_session(ua)
        self.session.auth = (cfg.username, cfg.application_password)
        parsed = urlparse(self.cfg.base_url)
        self.domain = parsed.netloc.split(":")[0]
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
        ids: List[int] = []
        for name in names:
            if not name:
                continue
            key = name.strip().lower()
            if key in self._category_cache:
                ids.append(self._category_cache[key])
                continue

            slug = self._slugify(name)
            try:
                r = self.session.get(self._categories_endpoint(), params={"slug": slug}, timeout=20)
                if r.status_code == 200:
                    arr = r.json()
                    if isinstance(arr, list) and arr:
                        cat_id = arr[0].get("id")
                        if cat_id:
                            self._category_cache[key] = cat_id
                            ids.append(cat_id)
                            logging.debug(f"[{self.cfg.name}] Using existing category '{name}' (id={cat_id})")
                            continue
            except Exception as e:
                logging.warning(f"[{self.cfg.name}] Category GET error for '{name}': {e}")

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
                    logging.error(f"[{self.cfg.name}] Failed to create category '{name}' status={r.status_code} resp={r.text[:300]}")
            except Exception as e:
                logging.error(f"[{self.cfg.name}] Category POST error for '{name}': {e}")

        return ids

    def _upload_and_rehost_images(self, html: str, guid: str, source_url: str) -> Tuple[str, Optional[int]]:
        if not html:
            logging.debug(f"[{self.cfg.name}] Empty HTML for GUID={guid}, skipping image rehost.")
            return html, None

        soup = BeautifulSoup(html, "html.parser")
        imgs = soup.find_all("img")
        logging.debug(f"[{self.cfg.name}] GUID={guid} images found in content: {len(imgs)}")
        if not imgs:
            return html, None

        featured_media_id = None

        for idx, img in enumerate(imgs):
            src = img.get("src")
            if not src:
                continue

            try:
                logging.debug(f"[{self.cfg.name}] GUID={guid} downloading image #{idx}: {src}")
                r_img = self.session.get(src, timeout=20, headers={"Referer": source_url})
                r_img.raise_for_status()
                img_bytes = r_img.content
            except Exception as e:
                logging.warning(f"[{self.cfg.name}] GUID={guid} image download failed ({src}): {e}")
                if hasattr(e, "response") and e.response is not None:
                    try:
                        logging.debug(f"[{self.cfg.name}] Image response first 200 chars: {e.response.text[:200]}")
                    except Exception:
                        pass
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
                logging.debug(f"[{self.cfg.name}] GUID={guid} uploading image to media: {filename}")
                r = self.session.post(self._media_endpoint(), files=files, headers=headers, timeout=40)
            except Exception as e:
                logging.error(f"[{self.cfg.name}] GUID={guid} media upload error: {e}")
                continue

            if r.status_code not in (200, 201):
                logging.error(f"[{self.cfg.name}] GUID={guid} media upload failed {r.status_code}: {r.text[:500]}")
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
        logging.debug(f"[{self.cfg.name}] GUID={guid} final HTML length after rehost: {len(final_html)}")
        return final_html, featured_media_id

    def create_post(self, item: PostItem, dry_run: bool = False) -> Tuple[bool, Optional[int], Optional[int]]:
        logging.debug(f"[{self.cfg.name}] Preparing post for GUID={item.guid}, TITLE={item.title!r}")

        rss_cat_ids = self._ensure_categories(item.rss_categories)
        all_cat_ids = list({*(rss_cat_ids or []), *(self.cfg.default_categories or [])})

        processed_html, featured_media_id = self._upload_and_rehost_images(item.content_html, item.guid, item.url)

        payload: Dict[str, Any] = {
            "title": item.title,
            "content": processed_html,
            "status": self.cfg.default_status
        }

        if item.published_at is not None:
            try:
                payload["date"] = item.published_at.isoformat()
            except Exception:
                pass

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
            f"[{self.cfg.name}] Creating post via {api_url} | GUID={item.guid} | content_len={len(processed_html)} cats={all_cat_ids}"
        )

        if dry_run:
            logging.info(f"[{self.cfg.name}] DRY RUN - would POST payload keys: {list(payload.keys())}")
            return True, None, None

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

        # Log detailed WP error info
        logging.error(f"[{self.cfg.name}] GUID={item.guid} post create failed {r.status_code}. Response:\n{r.text[:2000]}")
        # If 401/403, suggest checking auth
        if r.status_code in (401, 403):
            logging.error(f"[{self.cfg.name}] Authentication failed (status {r.status_code}). Check username/application_password.")
        return False, None, r.status_code


# ----------------- MAIN CONTROLLER -----------------
class AutoPoster:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        with open(config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # runtime
        self.runtime_cfg = RuntimeConfig(**raw.get("runtime", {}))
        # apply default user agent if blank
        if not (self.runtime_cfg.user_agent or "").strip():
            self.runtime_cfg.user_agent = os.environ.get("AUTOPOSTER_USER_AGENT", DEFAULT_USER_AGENT)

        # logging level
        lvl = getattr(logging, (self.runtime_cfg.log_level or "DEBUG").upper(), logging.DEBUG)
        logging.getLogger().setLevel(lvl)
        logging.debug(f"[INIT] Logging set to {logging.getLevelName(lvl)}")

        db_cfg = DBConfig(**raw["db"])
        self.db = DB(db_cfg)

        # support older config shapes (single source/target)
        sources_raw = raw.get("sources", [])
        self.sources_cfg = [SourceConfig(**s) for s in sources_raw]
        targets_raw = raw.get("targets", [])
        self.targets_cfg = [TargetConfig(**t) for t in targets_raw]

        self.fetchers = [SourceFetcher(cfg, self.runtime_cfg) for cfg in self.sources_cfg]
        self.clients: List[WordPressClient] = []
        self.site_tables: Dict[str, str] = {}
        self.domain_rss: Dict[str, str] = {}

        for t in self.targets_cfg:
            client = WordPressClient(t, self.runtime_cfg)
            self.clients.append(client)
            table_name = self.db.ensure_site_table(client.domain)
            self.site_tables[client.domain] = table_name
            logging.debug(f"[INIT] Site '{t.name}' domain '{client.domain}' uses table '{table_name}'")

            # Assign RSS to domain if not already set
            existing_rss = self.db.get_rss_for_domain(client.domain)
            if existing_rss:
                self.domain_rss[client.domain] = existing_rss
                logging.debug(f"[INIT] Domain '{client.domain}' already has RSS: {existing_rss}")
            else:
                # Assign the first available RSS from sources
                available_rss = self.sources_cfg[0].rss_url if self.sources_cfg else []
                if available_rss:
                    assigned_rss = available_rss[0]  # Take first RSS
                    self.db.set_rss_for_domain(client.domain, assigned_rss)
                    self.domain_rss[client.domain] = assigned_rss
                    logging.info(f"[INIT] Assigned RSS '{assigned_rss}' to domain '{client.domain}'")
                else:
                    logging.warning(f"[INIT] No RSS available to assign to domain '{client.domain}'")

    def run_forever(self, dry_run: bool = False, limit_feeds: Optional[int] = None):
        logging.info("=== WP AutoPoster (fixed) Started ===")
        while True:
            try:
                self.single_cycle(dry_run=dry_run, limit_feeds=limit_feeds)
            except KeyboardInterrupt:
                logging.info("Interrupted by user, exiting.")
                break
            except Exception as e:
                logging.error(f"Fatal error in main loop: {e}")
                traceback.print_exc()

            logging.info(f"Sleeping {self.runtime_cfg.poll_interval_seconds} seconds...")
            time.sleep(self.runtime_cfg.poll_interval_seconds)

    def single_cycle(self, dry_run: bool = False, limit_feeds: Optional[int] = None):
        logging.info("----- NEW CYCLE -----")

        for client in self.clients:
            domain = client.domain
            rss_url = self.domain_rss.get(domain)
            if not rss_url:
                logging.warning(f"[CYCLE] No RSS assigned to domain '{domain}', skipping.")
                continue

            # Create a temporary source config for this domain's RSS
            source_cfg = SourceConfig(
                mode="rss",
                rss_url=[rss_url],  # Keep as list for compatibility
                full_content_from_article_page=self.sources_cfg[0].full_content_from_article_page if self.sources_cfg else False,
                article_content_selector=self.sources_cfg[0].article_content_selector if self.sources_cfg else None,
                title_selector=self.sources_cfg[0].title_selector if self.sources_cfg else None,
                timezone=self.sources_cfg[0].timezone if self.sources_cfg else "UTC"
            )
            fetcher = SourceFetcher(source_cfg, self.runtime_cfg)

            try:
                new_items = fetcher.fetch_new(self.db, limit_feeds=limit_feeds)
            except Exception as e:
                logging.error(f"[CYCLE] Error while fetching for domain '{domain}': {e}")
                traceback.print_exc()
                continue

            if not new_items:
                logging.info(f"[CYCLE] No new posts for domain '{domain}'.")
                continue

            logging.info(f"[CYCLE] Domain '{domain}': {len(new_items)} new items")
            new_items = new_items[: self.runtime_cfg.max_posts_per_cycle]

            for item in new_items:
                try:
                    logging.info(f"[CYCLE] Processing GUID={item.guid}, TITLE={item.title!r} for domain '{domain}'")
                    self.db.insert_source_post(item)

                    table_name = self.site_tables[domain]

                    if self.db.site_post_exists(table_name, item.guid):
                        logging.info(f"[{client.cfg.name}] GUID={item.guid} already posted to this site, skipping.")
                        continue

                    success, wp_id, status_code = client.create_post(item, dry_run=dry_run)
                    self.db.log_push(item.guid, client.cfg.name, wp_id, status_code, success)

                    if success and wp_id:
                        self.db.mark_site_post(table_name, item.guid, wp_post_id=wp_id)
                        logging.info(f"[SUCCESS] Posted '{item.title}' to {client.cfg.name} (WP ID: {wp_id})")
                    elif success and wp_id is None:
                        logging.info(f"[INFO] (dry-run or no wp_id) Processed '{item.title}' for {client.cfg.name}")
                    else:
                        logging.error(f"[ERROR] Failed to post '{item.title}' to {client.cfg.name} (Status: {status_code})")
                except Exception as e:
                    logging.error(f"[CYCLE] Exception while processing item GUID={item.guid} for domain '{domain}': {e}")
                    traceback.print_exc()


# ----------------- CLI -----------------
def parse_args():
    p = argparse.ArgumentParser(description="WP AutoPoster (fixed)")
    p.add_argument("--config", "-c", default="config.json", help="Config JSON path")
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--dry-run", action="store_true", help="Do not actually POST to WordPress (media/posts)")
    p.add_argument("--limit-feeds", type=int, default=None, help="Limit number of RSS feeds to process (helpful for debug)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cfg_path = args.config

    if not os.path.exists(cfg_path):
        logging.critical(f"Config file not found: {cfg_path}")
        sys.exit(2)

    poster = AutoPoster(cfg_path)

    try:
        if args.once:
            poster.single_cycle(dry_run=args.dry_run, limit_feeds=args.limit_feeds)
        else:
            poster.run_forever(dry_run=args.dry_run, limit_feeds=args.limit_feeds)
    except Exception as e:
        logging.critical(f"Uncaught error: {e}")
        traceback.print_exc()
        sys.exit(1)
