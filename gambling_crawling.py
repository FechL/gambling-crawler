from ddgs import DDGS
import httpx
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# Configuration
BLOCKED_DOMAINS = ["wikipedia.org"]
OUTPUT_DIR = "/home/aliy/Coding/crawler/output"
OUTPUT_IMG_DIR = os.path.join(OUTPUT_DIR, "img")
LAST_ID_FILE = os.path.join(OUTPUT_DIR, "last_id.txt")
MAX_WORKERS = 5
VERSION = "1.0"

# Ensure output directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_IMG_DIR, exist_ok=True)


def get_last_id():
    """Get last ID from file or return -1 if not exists."""
    if os.path.exists(LAST_ID_FILE):
        try:
            with open(LAST_ID_FILE, "r") as f:
                return int(f.read().strip())
        except:
            return -1
    return -1


def save_last_id(last_id):
    """Save the last ID to file."""
    with open(LAST_ID_FILE, "w") as f:
        f.write(str(last_id))


def extract_domain(url):
    """Extract domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        return domain if domain else "unknown"
    except:
        return "unknown"


def get_og_data(html):
    """Extract OG meta tags from HTML."""
    try:
        soup = BeautifulSoup(html, "html.parser")

        def og(prop):
            tag = soup.find("meta", property=f"og:{prop}")
            return tag["content"] if tag and tag.get("content") else None

        return {
            "og:title": og("title"),
            "og:description": og("description"),
            "og:type": og("type"),
            "og:site_name": og("site_name"),
        }
    except Exception as e:
        return {
            "og:title": f"Error: {str(e)}",
            "og:description": f"Error: {str(e)}",
            "og:type": f"Error: {str(e)}",
            "og:site_name": f"Error: {str(e)}",
        }


def take_screenshot(url, output_path):
    """Take screenshot of the URL."""
    try:
        options = Options()
        options.binary_location = "/usr/bin/chromium-browser"
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(url)
        time.sleep(3)
        driver.save_screenshot(output_path)
        driver.quit()
        return True
    except Exception as e:
        print(f"Screenshot error for {url}: {e}")
        return False


def fetch_url_data(url_item, item_index, current_id):
    """Fetch data for a single URL."""
    result = {
        "id": f"{current_id:08d}",
        "title": url_item.get("title", "-"),
        "url": url_item.get("href", "-"),
        "domain": extract_domain(url_item.get("href", "-")),
        "description": url_item.get("body", "-"),
        "og_metadata": {},
    }

    url = url_item.get("href", "")

    if url:
        try:
            resp = httpx.get(url, timeout=8, follow_redirects=True)
            resp.raise_for_status()
            html = resp.text
            result["og_metadata"] = get_og_data(html)

            # Take screenshot
            screenshot_path = os.path.join(
                OUTPUT_IMG_DIR, f"{current_id:08d}.png"
            )
            take_screenshot(url, screenshot_path)

        except Exception as e:
            result["og_metadata"] = {
                "og:title": f"Error: {str(e)}",
                "og:description": f"Error: {str(e)}",
                "og:type": f"Error: {str(e)}",
                "og:site_name": f"Error: {str(e)}",
            }

    print(f"[{item_index}] Processed ID {current_id:08d}: {result['title'][:50]}")
    return result


def main():
    # Get input
    query = input("Masukkan keyword: ")
    print(f"\n=== PENCARIAN: {query} ===")

    # Get search results
    print("Fetching search results...")
    results = DDGS().text(query, max_results=10)

    # Filter blocked domains
    filtered = [
        r
        for r in results
        if not any(b in r.get("href", "") for b in BLOCKED_DOMAINS)
    ]

    if not filtered:
        print("Tidak ada hasil setelah filter.")
        return

    # Get last ID and start from next
    last_id = get_last_id()
    current_id = last_id + 1

    print(f"Starting ID: {current_id:08d}")
    print(f"Total URLs to process: {len(filtered)}")

    # Process URLs with multithreading
    all_results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_url_data, url_item, idx + 1, current_id + idx): idx
            for idx, url_item in enumerate(filtered)
        }

        for future in as_completed(futures):
            result = future.result()
            all_results.append(result)

    # Sort by ID to maintain order
    all_results.sort(key=lambda x: int(x["id"]))

    # Generate timestamp
    now = datetime.utcnow()
    timestamp_iso = now.isoformat() + "Z"
    timestamp_filename = now.strftime("%d%m%y-%H%M")

    # Prepare metadata
    metadata = {
        "metadata": {
            "total_records": len(all_results),
            "generated_at": timestamp_iso,
            "version": VERSION,
            "keyword": query,
        },
        "data": all_results,
    }

    # Save to JSON
    json_filename = f"{timestamp_filename}.json"
    json_filepath = os.path.join(OUTPUT_DIR, json_filename)

    with open(json_filepath, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"\n✓ JSON saved: {json_filepath}")

    # Update last ID
    final_id = current_id + len(all_results) - 1
    save_last_id(final_id)
    print(f"✓ Last ID updated: {final_id:08d} (saved in {LAST_ID_FILE})")

    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"Total records: {len(all_results)}")
    print(f"Generated at: {timestamp_iso}")
    print(f"Output file: {json_filepath}")
    print(f"Screenshots saved in: {OUTPUT_IMG_DIR}")


if __name__ == "__main__":
    main()
