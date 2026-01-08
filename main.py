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
from datetime import datetime, timedelta
import re
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONFIG_FILE = 'config.json'

def load_config():
    """Loads the configuration from config.json."""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def validate_config():
    """Validates the configuration file."""
    config = load_config()
    if not config.get('db'):
        raise ValueError("DB config missing")
    if not config.get('sources'):
        raise ValueError("Sources missing")
    for source in config['sources']:
        if not source.get('rss_url'):
            raise ValueError("RSS URL missing in source")
        if not source.get('domains'):
            raise ValueError("Domains missing in source")
        for domain in source['domains']:
            required = ['base_url', 'username', 'application_password']
            for req in required:
                if req not in domain:
                    raise ValueError(f"{req} missing in domain")
    logging.info("Config validation passed.")

def get_db_connection():
    config = load_config()
    db_config = config['db']
    client = MongoClient(db_config['connection_string'])
    return client[db_config['database']]

def setup_database():
    """Sets up the MongoDB database (collections are created automatically)."""
    db = get_db_connection()
    # Collections: posts, posted_records, failed_sites
    logging.info("Database setup complete.")

def get_category_id(base_url, username, password, category_name):
    """Get category ID by name from WordPress."""
    url = f"{base_url.rstrip('/')}/wp-json/wp/v2/categories?search={category_name}"
    try:
        res = requests.get(url, auth=(username, password), timeout=20)
        res.raise_for_status()
        cats = res.json()
        if cats:
            logging.info(f"Category '{category_name}' found on {base_url}, ID: {cats[0]['id']}")
            return cats[0]['id']
        else:
            logging.warning(f"Category '{category_name}' not found on {base_url}, attempting to create it.")
    except Exception as e:
        logging.error(f"Failed to get category ID for {category_name} on {base_url}: {e}")
    return None

def create_category(base_url, username, password, category_name):
    """Create a new category on WordPress and return its ID."""
    url = f"{base_url.rstrip('/')}/wp-json/wp/v2/categories"
    data = {'name': category_name}
    try:
        res = requests.post(url, json=data, auth=(username, password), timeout=20)
        res.raise_for_status()
        cat = res.json()
        logging.info(f"Created category '{category_name}' on {base_url}, ID: {cat['id']}")
        return cat['id']
    except Exception as e:
        logging.error(f"Failed to create category '{category_name}' on {base_url}: {e}")
    return None

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
        rss_url = source.get('rss_url')
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
    """Safely extracts text or content from an element using a CSS selector."""
    if not selector:
        return None
    element = soup.select_one(selector)
    if element:
        if element.name == 'meta':
            return element.get('content')
        else:
            return element.decode_contents()
    return None

def extract_image_url(soup, selector, base_url):
    """Safely extracts image URL from an element using a CSS selector."""
    if not selector:
        return None
    element = soup.select_one(selector)
    if element:
        if element.name == 'meta':
            src = element.get('content')
        elif element.name == 'img':
            src = element.get('src')
        else:
            src = None
        if src:
            if src.startswith('http'):
                return src
            else:
                return urljoin(base_url, src)
    return None

def clean_content(html):
    if not html:
        return ""
    soup = BeautifulSoup(html, 'html.parser')
    # Remove unwanted tags
    for tag in soup(['script', 'style', 'nav', 'aside', 'footer', 'header', 'iframe', 'form', 'noscript']):
        tag.decompose()
    # Remove elements with certain classes or ids
    for tag in soup.find_all(attrs={'class': re.compile(r'ad|ads|advertisement|social|share|related|sidebar|popup|modal', re.I)}):
        tag.decompose()
    for tag in soup.find_all(attrs={'id': re.compile(r'ad|ads|advertisement|social|share|related|sidebar|popup|modal', re.I)}):
        tag.decompose()
    # Remove empty tags
    for tag in soup.find_all():
        if not tag.get_text(strip=True) and tag.name not in ['img', 'br', 'hr', 'p']:
            tag.decompose()
    return str(soup)

def upload_image(base_url, username, password, image_url):
    """Downloads and uploads an image to WordPress media library."""
    # Download image
    scraper = cloudscraper.create_scraper()
    img_response = scraper.get(image_url, timeout=15)
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

def process_single_post(post_doc):
    """Processes a single pending post from the database and posts it to the corresponding WordPress site."""
    db = get_db_connection()
    posts_collection = db['posts']
    posted_records_collection = db['posted_records']

    post_id = post_doc['_id']
    post_url = post_doc['post_url']
    rss_url = post_doc['rss_url']
    slug = post_doc['slug']
    logging.info(f"Processing post {post_id}: {post_url}")

    config = load_config()
    source_config = None

    for source in config.get('sources', []):
        if source.get('rss_url') == rss_url:
            source_config = source
            break

    if not source_config:
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

        title = extract_element(soup, source_config.get('title_selector'))
        content = extract_element(soup, source_config.get('content_selector'))
        content = clean_content(content)
        # time can be handled more specifically if needed (e.g., parsing datetime)
        post_time = extract_element(soup, source_config.get('time_selector'))
        image_url = extract_image_url(soup, source_config.get('featured_image_selector'), post_url)
        if image_url:
            logging.info(f"Found featured image: {image_url}")
        else:
            logging.info("No featured image found")

        logging.info(f"Extracted title: {title[:50] if title else 'None'}")
        logging.info(f"Extracted content length: {len(content) if content else 0}")

        if not title or not content:
            logging.error("Failed to extract title or content.")
            posts_collection.delete_one({'_id': post_id})
            return

        domains = source_config.get('domains', [])

        failed_sites_collection = db['failed_sites']
        active_domains = []
        for domain in domains:
            base_url = domain['base_url']
            failed_doc = failed_sites_collection.find_one({'site_url': base_url})
            if failed_doc and failed_doc['failed_at'] > datetime.utcnow() - timedelta(hours=1):
                logging.info(f"Skipping recently failed site: {base_url}")
                continue
            active_domains.append(domain)

        if not active_domains:
            logging.info("All sites failed recently, marking post as failed.")
            posts_collection.update_one({'_id': post_id}, {'$set': {'failed_at': datetime.utcnow()}})
            return

        successful_posts = 0
        for domain in active_domains:
            base_url = domain['base_url']
            if posted_records_collection.find_one({'site_url': base_url, 'slug': slug}):
                logging.info(f"Already posted {slug} to {base_url}, skipping.")
                successful_posts += 1
                continue

            username = domain['username']
            password = domain['application_password']
            categories_ids = []
            # Add default categories
            for cat_name in source_config.get('default_categories', []):
                cat_id = get_category_id(base_url, username, password, cat_name)
                if not cat_id:
                    cat_id = create_category(base_url, username, password, cat_name)
                if cat_id:
                    categories_ids.append(cat_id)
            # Add domain specific category
            category_name = domain.get('category', 'uncategorized')
            category_id = get_category_id(base_url, username, password, category_name)
            if not category_id:
                category_id = create_category(base_url, username, password, category_name)
                if not category_id:
                    # Category creation failed, skip this site for 5 minutes
                    failed_sites_collection.update_one(
                        {'site_url': base_url},
                        {'$set': {'failed_at': datetime.utcnow() - timedelta(minutes=55)}},
                        upsert=True
                    )
                    logging.warning(f"Skipping {base_url} for 5 minutes due to category error.")
                    continue
            if category_id:
                categories_ids.append(category_id)
            post_data = {
                'title': title,
                'content': content,
                'status': source_config.get('default_status', 'publish'),
                'slug': slug.replace('.cms', ''),
                'categories': categories_ids,
                'tags': source_config.get('default_tags', []),
            }
            if post_time:
                try:
                    post_time_dt = datetime.fromisoformat(post_time)
                    post_data['date'] = post_time_dt.isoformat()
                except ValueError:
                    logging.warning(f"Could not parse date: {post_time}")

            # Upload featured image if available
            if image_url:
                try:
                    media_id = upload_image(base_url, username, password, image_url)
                    post_data['featured_media'] = media_id
                    logging.info(f"Uploaded featured image to {base_url}")
                except Exception as e:
                    logging.error(f"Failed to upload featured image to {base_url}: {e}")

            wp_api_url = f"{base_url.rstrip('/')}/wp-json/wp/v2/posts"
            logging.info(f"Posting to {wp_api_url} with data: {post_data}")
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
                if res.status_code >= 400:
                    logging.error(f"Response body: {res.text[:500]}")
                res.raise_for_status()
                posted_records_collection.insert_one({
                    'site_url': base_url,
                    'slug': slug,
                    'posted_at': datetime.utcnow()
                })
                failed_sites_collection.delete_one({'site_url': base_url})
                successful_posts += 1
                try:
                    post_response = res.json()
                    logging.info(f"Successfully posted to {base_url}. Post ID: {post_response.get('id')}")
                except ValueError:
                    logging.error(f"Posted to {base_url} but response is not JSON. Response: {res.text[:200]}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to post to {base_url}: {e}")
                if hasattr(e, 'response') and e.response:
                    logging.error(f"Response body: {e.response.text[:500]}")
                failed_sites_collection.update_one(
                    {'site_url': base_url},
                    {'$set': {'failed_at': datetime.utcnow()}},
                    upsert=True
                )

        if successful_posts == len(active_domains):
            posts_collection.update_one({'_id': post_id}, {'$unset': {'failed_at': 1}})
            posts_collection.delete_one({'_id': post_id})
            logging.info(f"Successfully processed and posted {slug} to all sites.")
        elif successful_posts == 0:
            posts_collection.update_one({'_id': post_id}, {'$set': {'failed_at': datetime.utcnow()}})
            logging.error(f"Failed to post {slug} to any site. Will retry later.")
        else:
            logging.info(f"Posted {slug} to {successful_posts}/{len(active_domains)} sites. Will retry for remaining.")

    except Exception as e:
        logging.error(f"Error processing post {post_url}: {e}")
        posts_collection.update_one({'_id': post_id}, {'$set': {'failed_at': datetime.utcnow()}})

def process_multiple_posts():
    """Processes up to 10 pending posts concurrently."""
    logging.info("Checking for pending posts...")
    db = get_db_connection()
    posts_collection = db['posts']

    filter_query = {'$or': [{'failed_at': {'$exists': False}}, {'failed_at': {'$lt': datetime.utcnow() - timedelta(minutes=30)}}]}
    pending_posts = list(posts_collection.find(filter_query, sort=[('created_at', 1)], limit=30))

    if not pending_posts:
        logging.info("No pending posts found.")
        return

    threads = []
    for post_doc in pending_posts:
        t = threading.Thread(target=process_single_post, args=(post_doc,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    logging.info(f"Processed {len(pending_posts)} posts.")

def main():
    """Main function to set up the database and schedule the jobs."""
    validate_config()
    setup_database()

    # Run initial poll to populate DB
    logging.info("Running initial RSS poll...")
    poll_rss_feeds()

    # Run initial process
    logging.info("Running initial post processing...")
    process_multiple_posts()

    scheduler = BackgroundScheduler()
    # Using misfire_grace_time to prevent job from running multiple times if script is busy
    scheduler.add_job(poll_rss_feeds, 'interval', minutes=10, misfire_grace_time=3600)
    scheduler.add_job(process_multiple_posts, 'interval', seconds=10, misfire_grace_time=5)
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
