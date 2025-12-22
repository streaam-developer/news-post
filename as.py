#!/usr/bin/env python3
import json
import time
import uuid
import logging
import traceback
import requests
import mysql.connector
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------- DATA CLASSES ----------------
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
    timezone: str = "UTC"


@dataclass
class TargetConfig:
    name: str
    base_url: str
    username: str
    application_password: str
    default_status: str = "publish"
    post_type: str = "posts"


@dataclass
class RuntimeConfig:
    poll_interval_seconds: int = 600
    max_workers: int = 5
    log_level: str = "INFO"
    user_agent: str = "Mozilla/5.0"



@dataclass
class PostItem:
    guid: str
    url: str
    title: str
    published_at: Optional[datetime] = None


# ---------------- DATABASE ----------------
class DB:
    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self.conn = mysql.connector.connect(
            host=cfg.host,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            port=cfg.port,
            autocommit=False
        )
        self._init_tables()

    def cursor(self, dict=False):
        return self.conn.cursor(dictionary=dict)

    def _init_tables(self):
        cur = self.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS source_posts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            guid VARCHAR(255) UNIQUE,
            url TEXT,
            title TEXT,
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS post_push_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            guid VARCHAR(255),
            domain VARCHAR(255),
            success TINYINT(1),
            cycle_id VARCHAR(64),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_guid_domain (guid, domain)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cycle_domain_block (
            cycle_id VARCHAR(64),
            domain VARCHAR(255),
            PRIMARY KEY (cycle_id, domain)
        )
        """)
        self.conn.commit()

    # ---------- SOURCE POSTS ----------
    def has_source_post(self, guid):
        cur = self.cursor()
        cur.execute("SELECT 1 FROM source_posts WHERE guid=%s", (guid,))
        return cur.fetchone() is not None

    def insert_source_post(self, item: PostItem):
        cur = self.cursor()
        cur.execute(
            "INSERT IGNORE INTO source_posts (guid,url,title) VALUES (%s,%s,%s)",
            (item.guid, item.url, item.title)
        )
        self.conn.commit()

    def get_all_source_posts(self):
        cur = self.cursor(dict=True)
        cur.execute("SELECT * FROM source_posts ORDER BY id ASC")
        return cur.fetchall()

    # ---------- DOMAIN BLOCK ----------
    def is_domain_blocked(self, cycle_id, domain):
        cur = self.cursor()
        cur.execute(
            "SELECT 1 FROM cycle_domain_block WHERE cycle_id=%s AND domain=%s",
            (cycle_id, domain)
        )
        return cur.fetchone() is not None

    def block_domain(self, cycle_id, domain):
        cur = self.cursor()
        cur.execute(
            "INSERT IGNORE INTO cycle_domain_block (cycle_id,domain) VALUES (%s,%s)",
            (cycle_id, domain)
        )
        self.conn.commit()

    # ---------- LOG ----------
    def log_push(self, guid, domain, success, cycle_id):
        cur = self.cursor()
        cur.execute(
            "INSERT IGNORE INTO post_push_log (guid,domain,success,cycle_id) VALUES (%s,%s,%s,%s)",
            (guid, domain, int(success), cycle_id)
        )
        self.conn.commit()


# ---------------- RSS COLLECTOR ----------------
class RSSCollector:
    def __init__(self, cfg: SourceConfig):
        self.cfg = cfg

    def collect(self, db: DB):
        for rss in self.cfg.rss_url:
            logging.info(f"[RSS] Fetching {rss}")
            try:
                r = requests.get(rss, timeout=20)
                soup = BeautifulSoup(r.text, "xml")
                for it in soup.find_all("item"):
                    guid = it.guid.text if it.guid else it.link.text
                    url = it.link.text
                    title = it.title.text if it.title else "(no title)"
                    if not db.has_source_post(guid):
                        db.insert_source_post(PostItem(guid, url, title))
            except Exception as e:
                logging.error(f"[RSS] Error {rss} → {e}")


# ---------------- WORDPRESS ----------------
class WordPressClient:
    def __init__(self, cfg: TargetConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.auth = (cfg.username, cfg.application_password)
        self.domain = urlparse(cfg.base_url).netloc

    def create_post(self, post: Dict[str, Any]):
        try:
            api = f"{self.cfg.base_url.rstrip('/')}/wp-json/wp/v2/{self.cfg.post_type}"
            r = self.session.post(
                api,
                json={
                    "title": post["title"],
                    "content": f"<p><a href='{post['url']}'>Source</a></p>",
                    "status": self.cfg.default_status
                },
                timeout=40
            )
            return r.status_code in (200, 201)
        except Exception:
            return False


# ---------------- MAIN CONTROLLER ----------------
class AutoPoster:
    def __init__(self, config_path="config.json"):
        raw = json.load(open(config_path))

        self.runtime = RuntimeConfig(**raw["runtime"])
        logging.getLogger().setLevel(self.runtime.log_level)

        self.db_cfg = DBConfig(**raw["db"])
        self.db = DB(self.db_cfg)

        self.sources = [SourceConfig(**s) for s in raw["sources"]]
        self.targets = [TargetConfig(**t) for t in raw["targets"]]

        self.collectors = [RSSCollector(s) for s in self.sources]
        self.clients = [WordPressClient(t) for t in self.targets]

    def run_forever(self):
        while True:
            cycle_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            logging.info(f"🚀 NEW CYCLE {cycle_id}")

            # 1️⃣ RSS → DB
            for c in self.collectors:
                c.collect(self.db)

            posts = self.db.get_all_source_posts()

            # 2️⃣ DB → DOMAINS
            with ThreadPoolExecutor(max_workers=self.runtime.max_workers) as ex:
                for client in self.clients:
                    ex.submit(self._process_domain, client, posts, cycle_id)

            logging.info("⏳ Cycle done → sleeping 10 min")
            time.sleep(self.runtime.poll_interval_seconds)

    def _process_domain(self, client, posts, cycle_id):
        db = DB(self.db_cfg)
        domain = client.domain

        if db.is_domain_blocked(cycle_id, domain):
            return

        for post in posts:
            success = client.create_post(post)
            db.log_push(post["guid"], domain, success, cycle_id)

            if not success:
                logging.error(f"[{domain}] FAILED → BLOCKED FOR CYCLE")
                db.block_domain(cycle_id, domain)
                break

            logging.info(f"[{domain}] Posted: {post['title']}")


# ---------------- RUN ----------------
if __name__ == "__main__":
    AutoPoster("config.json").run_forever()
