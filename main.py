import json
import mysql.connector
import time
import feedparser
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONFIG_FILE = 'config.json'

def load_config():
    """Loads the configuration from config.json."""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def get_db_connection():
    config = load_config()
    db_config = config['db']
    return mysql.connector.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )

def setup_database():
    """Sets up the MySQL database and creates the tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            post_url VARCHAR(500) NOT NULL UNIQUE,
            rss_url VARCHAR(500) NOT NULL,
            slug VARCHAR(255) NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posted_slugs (
            slug VARCHAR(255) PRIMARY KEY,
            posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logging.info("Database setup complete.")

def get_slug_from_url(url):
    """Generates a simple slug from a URL."""
    return url.strip('/').split('/')[-1]

def poll_rss_feeds():
    """Polls all RSS feeds from the config and stores new post links in the database."""
    logging.info("Polling RSS feeds...")
    config = load_config()
    conn = get_db_connection()
    cursor = conn.cursor()

    for source in config.get('sources', []):
        for site in source.get('sites', []):
            rss_url = site.get('rss_url')
            if not rss_url:
                continue

            try:
                feed = feedparser.parse(rss_url)
                for entry in feed.entries:
                    post_url = entry.link
                    slug = get_slug_from_url(post_url)

                    # Check for duplicates
                    cursor.execute("SELECT id FROM posts WHERE post_url = %s OR slug = %s", (post_url, slug))
                    if cursor.fetchone() is None:
                        # Insert new post
                        cursor.execute(
                            "INSERT INTO posts (post_url, rss_url, slug) VALUES (%s, %s, %s)",
                            (post_url, rss_url, slug)
                        )
                        logging.info(f"New post found and stored: {post_url}")

            except Exception as e:
                logging.error(f"Error polling feed {rss_url}: {e}")

    conn.commit()
    conn.close()
    logging.info("Finished polling RSS feeds.")

def extract_element(soup, selector):
    """Safely extracts text from an element using a CSS selector."""
    if not selector:
        return None
    element = soup.select_one(selector)
    return element.get_text(strip=True) if element else None

def process_and_post():
    """Processes one pending post from the database and posts it to the corresponding WordPress site."""
    logging.info("Checking for pending posts...")
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM posts ORDER BY created_at ASC LIMIT 1")
    post_to_process = cursor.fetchone()

    if post_to_process is None:
        logging.info("No pending posts found.")
        conn.close()
        return

    post_id = post_to_process['id']
    post_url = post_to_process['post_url']
    rss_url = post_to_process['rss_url']
    slug = post_to_process['slug']
    logging.info(f"Processing post {post_id}: {post_url}")

    # Check if slug already posted
    cursor.execute("SELECT slug FROM posted_slugs WHERE slug = %s", (slug,))
    if cursor.fetchone():
        logging.info(f"Slug {slug} already posted, skipping.")
        cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
        conn.commit()
        conn.close()
        return

    config = load_config()
    site_config = None
    source_config = None

    for source in config.get('sources', []):
        for site in source.get('sites', []):
            if site.get('rss_url') == rss_url:
                site_config = site
                source_config = source
                break
        if site_config:
            break

    if not site_config or not source_config:
        logging.error(f"No configuration found for RSS feed: {rss_url}")
        cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
        conn.commit()
        conn.close()
        return

    try:
        response = requests.get(post_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        title = extract_element(soup, site_config.get('title_selector'))
        content = extract_element(soup, site_config.get('content_selector'))
        # time can be handled more specifically if needed (e.g., parsing datetime)
        post_time = extract_element(soup, site_config.get('time_selector'))

        if not title or not content:
            raise ValueError("Failed to extract title or content.")

        username = source_config['username']
        password = source_config['application_password']
        base_urls = site_config['base_url']
        if isinstance(base_urls, str):
            base_urls = [base_urls]

        post_data = {
            'title': title,
            'content': content,
            'status': source_config.get('default_status', 'publish'),
            # 'date': post_time # This might need parsing and formatting
        }

        all_posted_successfully = True
        for base_url in base_urls:
            wp_api_url = f"{base_url.rstrip('/')}/wp-json/wp/v2/posts"
            try:
                res = requests.post(
                    wp_api_url,
                    json=post_data,
                    auth=(username, password),
                    timeout=20
                )
                res.raise_for_status()
                logging.info(f"Successfully posted to {base_url}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to post to {base_url}: {e}")
                all_posted_successfully = False

        if all_posted_successfully:
            cursor.execute("INSERT INTO posted_slugs (slug) VALUES (%s)", (slug,))
            cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
            logging.info(f"Successfully processed and posted: {post_url}")
        else:
            raise Exception("Failed to post to one or more sites.")

    except Exception as e:
        logging.error(f"Error processing post {post_url}: {e}")
        cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))

    finally:
        conn.commit()
        conn.close()

def main():
    """Main function to set up the database and schedule the jobs."""
    setup_database()

    scheduler = BackgroundScheduler()
    # Using misfire_grace_time to prevent job from running multiple times if script is busy
    scheduler.add_job(poll_rss_feeds, 'interval', hours=1, misfire_grace_time=3600)
    scheduler.add_job(process_and_post, 'interval', minutes=10, misfire_grace_time=600)
    scheduler.start()

    logging.info("Scheduler started. Press Ctrl+C to exit.")

    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Scheduler shut down.")

if __name__ == '__main__':
    main()
