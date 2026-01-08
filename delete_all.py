import json
import requests
from pymongo import MongoClient
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
    client = MongoClient(db_config['connection_string'])
    return client[db_config['database']]

def get_all_domains():
    """Get unique list of domains from config."""
    config = load_config()
    domains = set()
    for source in config.get('sources', []):
        for domain in source.get('domains', []):
            domains.add((domain['base_url'], domain['username'], domain['application_password']))
    return list(domains)

def delete_all_posts_and_images(base_url, username, password):
    """Delete all posts and their featured images from a WordPress site."""
    logging.info(f"Deleting all posts and images from {base_url}")

    # Get all posts
    posts_url = f"{base_url.rstrip('/')}/wp-json/wp/v2/posts?per_page=100"
    try:
        res = requests.get(posts_url, auth=(username, password), timeout=30)
        res.raise_for_status()
        posts = res.json()
    except Exception as e:
        logging.error(f"Failed to get posts from {base_url}: {e}")
        return

    for post in posts:
        post_id = post['id']
        featured_media = post.get('featured_media')

        # Delete featured image if exists
        if featured_media:
            media_url = f"{base_url.rstrip('/')}/wp-json/wp/v2/media/{featured_media}"
            try:
                requests.delete(media_url, auth=(username, password), timeout=10)
                logging.info(f"Deleted media {featured_media} from {base_url}")
            except Exception as e:
                logging.error(f"Failed to delete media {featured_media} from {base_url}: {e}")

        # Delete post
        post_url = f"{base_url.rstrip('/')}/wp-json/wp/v2/posts/{post_id}"
        try:
            requests.delete(post_url, auth=(username, password), timeout=10)
            logging.info(f"Deleted post {post_id} from {base_url}")
        except Exception as e:
            logging.error(f"Failed to delete post {post_id} from {base_url}: {e}")

def clear_database():
    """Clear all records from the database."""
    logging.info("Clearing database...")
    db = get_db_connection()
    collections = ['posts', 'posted_records', 'failed_sites']
    for col in collections:
        db[col].delete_many({})
        logging.info(f"Cleared collection: {col}")

def main():
    """Main function to delete all posts and images from all domains and clear DB."""
    domains = get_all_domains()
    for base_url, username, password in domains:
        delete_all_posts_and_images(base_url, username, password)
    clear_database()
    logging.info("All posts, images, and database records deleted.")

if __name__ == '__main__':
    main()