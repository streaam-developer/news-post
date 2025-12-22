import requests
import cloudscraper

url = "https://www.ndtv.com/entertainment/boney-kapoor-says-he-originally-planned-to-make-aamir-khans-ghajini-with-salman-khan-regret-missing-out-on-the-opportunity-9876641#publisher=newsstand"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': 'https://www.ndtv.com/',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
}

try:
    session = requests.Session()
    response = session.get(url, headers=headers, timeout=15)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print("Success")
    else:
        print(f"Error: {response.status_code}")
except Exception as e:
    print(f"Exception: {e}")