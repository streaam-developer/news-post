import json
from pymongo import MongoClient
import time
import feedparser
import requests
import cloudscraper
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from urllib.parse import urljoin, urlparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONFIG_FILE = 'config.json'

def load_config():
    """Loads the configuration from config.json."""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def get_db_connection():
    config = load_config()
    db_config = config['db']
    client = MongoClient(db_config['connection_string'])
    return client[db_config['database']]

def setup_database():
    """Sets up the MongoDB database (collections are created automatically)."""
    db = get_db_connection()
    # Collections: posts and posted_slugs
    logging.info("Database setup complete.")

def get_slug_from_url(url):
    """Generates a simple slug from a URL."""
    return url.strip('/').split('/')[-1]

def poll_rss_feeds():
    """Polls all RSS feeds from the config and stores new post links in the database."""
    logging.info("Polling RSS feeds...")
    config = load_config()
    db = get_db_connection()
    posts_collection = db['posts']

    for source in config.get('sources', []):
        logging.info(f"Processing source: {source.get('username')}")
        for site in source.get('sites', []):
            rss_url = site.get('rss_url')
            logging.info(f"Polling RSS feed: {rss_url}")
            if not rss_url:
                continue

            try:
                feed = feedparser.parse(rss_url)
                logging.info(f"Found {len(feed.entries)} entries in feed")
                for entry in feed.entries:
                    post_url = entry.link
                    slug = get_slug_from_url(post_url)
                    logging.debug(f"Processing entry: {post_url}, slug: {slug}")

                    # Check for duplicates
                    if posts_collection.find_one({'post_url': post_url}):
                        logging.debug(f"Post already exists: {post_url}")
                    else:
                        # Insert new post
                        posts_collection.insert_one({
                            'post_url': post_url,
                            'rss_url': rss_url,
                            'slug': slug,
                            'created_at': datetime.utcnow()
                        })
                        logging.info(f"New post found and stored: {post_url}")

            except Exception as e:
                logging.error(f"Error polling feed {rss_url}: {e}")

    logging.info("Finished polling RSS feeds.")

def extract_element(soup, selector):
    """Safely extracts text from an element using a CSS selector."""
    if not selector:
        return None
    element = soup.select_one(selector)
    return element.get_text(strip=True) if element else None

def extract_image_url(soup, selector, base_url):
    """Safely extracts image URL from an element using a CSS selector."""
    if not selector:
        return None
    element = soup.select_one(selector)
    if element and element.name == 'img':
        src = element.get('src')
        if src:
            if src.startswith('http'):
                return src
            else:
                return urljoin(base_url, src)
    return None

def upload_image(base_url, username, password, image_url):
    """Downloads and uploads an image to WordPress media library."""
    # Download image
    img_response = requests.get(image_url, timeout=15)
    img_response.raise_for_status()

    # Get filename
    filename = image_url.split('/')[-1]
    if not filename or '.' not in filename:
        filename = 'image.jpg'

    # Upload to WP
    media_url = f"{base_url.rstrip('/')}/wp-json/wp/v2/media"
    content_type = img_response.headers.get('content-type', 'image/jpeg')
    files = {'file': (filename, img_response.content, content_type)}
    res = requests.post(media_url, files=files, auth=(username, password), timeout=20)
    res.raise_for_status()
    media_data = res.json()
    return media_data['id']

def process_and_post():
    """Processes one pending post from the database and posts it to the corresponding WordPress site."""
    logging.info("Checking for pending posts...")
    db = get_db_connection()
    posts_collection = db['posts']
    posted_slugs_collection = db['posted_slugs']

    post_to_process = posts_collection.find_one(sort=[('created_at', 1)])

    if post_to_process is None:
        logging.info("No pending posts found.")
        return

    post_id = post_to_process['_id']
    post_url = post_to_process['post_url']
    rss_url = post_to_process['rss_url']
    slug = post_to_process['slug']
    logging.info(f"Processing post {post_id}: {post_url}")

    # Check if slug already posted
    if posted_slugs_collection.find_one({'slug': slug}):
        logging.info(f"Slug {slug} already posted, skipping.")
        posts_collection.delete_one({'_id': post_id})
        return

    logging.info(f"Slug {slug} not posted yet, proceeding.")

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
        posts_collection.delete_one({'_id': post_id})
        return

    try:
        domain = urlparse(post_url).netloc
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': f'https://{domain}/',
        }
        response = requests.get(post_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        title = extract_element(soup, site_config.get('title_selector'))
        content = extract_element(soup, site_config.get('content_selector'))
        # time can be handled more specifically if needed (e.g., parsing datetime)
        post_time = extract_element(soup, site_config.get('time_selector'))
        image_url = extract_image_url(soup, site_config.get('featured_image_selector'), post_url)

        logging.info(f"Extracted title: {title[:50] if title else 'None'}")
        logging.info(f"Extracted content length: {len(content) if content else 0}")

        if not title or not content:
            logging.error("Failed to extract title or content.")
            posts_collection.delete_one({'_id': post_id})
            return

        username = source_config['username']
        password = source_config['application_password']
        base_urls = site_config['base_url']
        if isinstance(base_urls, str):
            base_urls = [base_urls]

        all_posted_successfully = True
        for base_url in base_urls:
            post_data = {
                'title': title,
                'content': content,
                'status': source_config.get('default_status', 'publish'),
                'slug': slug.replace('.cms', ''),
                'categories': source_config.get('default_categories', []),
                'tags': source_config.get('default_tags', []),
                # 'date': post_time # This might need parsing and formatting
            }

            # Upload featured image if available
            if image_url:
                try:
                    media_id = upload_image(base_url, username, password, image_url)
                    post_data['featured_media'] = media_id
                    logging.info(f"Uploaded featured image to {base_url}")
                except Exception as e:
                    logging.error(f"Failed to upload featured image to {base_url}: {e}")

            wp_api_url = f"{base_url.rstrip('/')}/wp-json/wp/v2/posts"
            try:
                res = requests.post(
                    wp_api_url,
                    json=post_data,
                    auth=(username, password),
                    timeout=20,
                    headers={'Content-Type': 'application/json'}
                )
                logging.info(f"Response status code from {base_url}: {res.status_code}")
                logging.info(f"Response headers from {base_url}: {res.headers}")
                res.raise_for_status()
                try:
                    post_response = res.json()
                    logging.info(f"Successfully posted to {base_url}. Post ID: {post_response.get('id')}")
                except ValueError:
                    logging.error(f"Posted to {base_url} but response is not JSON. Response: {res.text[:200]}")
                    all_posted_successfully = False
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to post to {base_url}: {e}")
                all_posted_successfully = False

        if all_posted_successfully:
            posted_slugs_collection.insert_one({
                'slug': slug,
                'posted_at': datetime.utcnow()
            })
            posts_collection.delete_one({'_id': post_id})
            logging.info(f"Successfully processed and posted: {post_url}")
        else:
            logging.error(f"Failed to post to one or more sites for {post_url}. Will retry later.")

    except Exception as e:
        logging.error(f"Error processing post {post_url}: {e}")
        posts_collection.delete_one({'_id': post_id})

def main():
    """Main function to set up the database and schedule the jobs."""
    setup_database()

    # Run initial poll to populate DB
    logging.info("Running initial RSS poll...")
    poll_rss_feeds()

    # Run initial process
    logging.info("Running initial post processing...")
    process_and_post()

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
