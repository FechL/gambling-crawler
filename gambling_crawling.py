from ddgs import DDGS
import httpx
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from multiprocessing import Pool, cpu_count

# Configuration
BLOCKED_DOMAINS = ["wikipedia.org"]
OUTPUT_DIR = "/home/aliy/Coding/crawler/output"
OUTPUT_IMG_DIR = os.path.join(OUTPUT_DIR, "img")
LAST_ID_FILE = os.path.join(OUTPUT_DIR, "last_id.txt")
ALL_DOMAINS_FILE = os.path.join(OUTPUT_DIR, "all_domains.txt")
MAX_WORKERS_FETCH = 5
MAX_WORKERS_SCREENSHOT = max(2, cpu_count() - 1)  # Use multiple CPU cores for screenshots
VERSION = "1.3"

# Global set untuk tracking domain (anti-duplikasi)
SEEN_DOMAINS = set()

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


def load_seen_domains():
    """Load all previously seen domains into global set."""
    global SEEN_DOMAINS
    SEEN_DOMAINS = set()
    
    if os.path.exists(ALL_DOMAINS_FILE):
        try:
            with open(ALL_DOMAINS_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    domain = line.strip()
                    if domain:
                        SEEN_DOMAINS.add(domain)
            print(f"[INFO] Loaded {len(SEEN_DOMAINS)} existing domains from {ALL_DOMAINS_FILE}")
        except Exception as e:
            print(f"[WARNING] Failed to load domains: {str(e)}")
    else:
        print(f"[INFO] No existing domains file. Starting fresh.")


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


def is_domain_duplicate(domain):
    """Check if domain already exists in global set."""
    return domain in SEEN_DOMAINS


def add_domain_to_set(domain):
    """Add domain to global set."""
    if domain and domain != "unknown":
        SEEN_DOMAINS.add(domain)


def save_new_domains(new_domains):
    """Append new domains to all_domains.txt file."""
    if not new_domains:
        return
    
    try:
        with open(ALL_DOMAINS_FILE, "a", encoding="utf-8") as f:
            for domain in new_domains:
                f.write(domain + "\n")
        print(f"[INFO] Saved {len(new_domains)} new domains to {ALL_DOMAINS_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to save new domains: {str(e)}")


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


def take_screenshot_worker(url, output_path, item_id):
    """Worker function for taking screenshot (must be picklable for multiprocessing)."""
    for attempt in range(3):
        try:
            options = Options()
            options.binary_location = "/usr/bin/chromium-browser"
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-software-rasterizer")
            options.add_argument("--disable-extensions")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
            
            service = Service("/usr/bin/chromedriver")
            driver = webdriver.Chrome(service=service, options=options)
            
            # Set timeouts
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            
            driver.get(url)
            time.sleep(5)
            driver.save_screenshot(output_path)
            driver.quit()
            
            return True
            
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                return False


def take_screenshot(url, output_path, retries=2):
    """Take screenshot of the URL with retry mechanism."""
    for attempt in range(retries + 1):
        try:
            options = Options()
            options.binary_location = "/usr/bin/chromium-browser"
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-software-rasterizer")
            options.add_argument("--disable-extensions")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
            
            service = Service("/usr/bin/chromedriver")
            driver = webdriver.Chrome(service=service, options=options)
            
            # Set timeouts
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            
            driver.get(url)
            
            # Wait for page to load (5-8 seconds)
            time.sleep(5)
            
            driver.save_screenshot(output_path)
            driver.quit()
            
            return True
            
        except Exception as e:
            if attempt < retries:
                print(f"Screenshot attempt {attempt + 1}/{retries + 1} failed for {url}: {str(e)[:100]}. Retrying...")
                time.sleep(2)
            else:
                print(f"Screenshot failed after {retries + 1} attempts for {url}: {str(e)[:100]}")
                return False


def fetch_url_data(url_item, item_index, current_id):
    """Fetch data for a single URL (without screenshot)."""
    result = {
        "id": f"{current_id:08d}",
        "title": url_item.get("title", "-"),
        "url": url_item.get("href", "-"),
        "domain": extract_domain(url_item.get("href", "-")),
        "description": url_item.get("body", "-"),
        "og_metadata": {},
        "screenshot_status": "pending",
    }

    url = url_item.get("href", "")

    if url:
        try:
            resp = httpx.get(url, timeout=10, follow_redirects=True)
            resp.raise_for_status()
            html = resp.text
            result["og_metadata"] = get_og_data(html)

        except Exception as e:
            result["og_metadata"] = {
                "og:title": f"Error: {str(e)}",
                "og:description": f"Error: {str(e)}",
                "og:type": f"Error: {str(e)}",
                "og:site_name": f"Error: {str(e)}",
            }

    print(f"[{item_index}] Fetched ID {current_id:08d}: {result['title'][:50]}")
    return result


def process_screenshots_parallel(all_results):
    """Process screenshots in parallel using multiprocessing."""
    print(f"\n=== TAKING SCREENSHOTS (Parallel - {MAX_WORKERS_SCREENSHOT} processes) ===")
    
    # Prepare screenshot tasks
    screenshot_tasks = []
    for result in all_results:
        url = result.get("url", "")
        item_id = result.get("id", "unknown")
        
        if url and url != "-":
            screenshot_path = os.path.join(OUTPUT_IMG_DIR, f"{item_id}.png")
            screenshot_tasks.append((url, screenshot_path, item_id))
    
    # Use ProcessPoolExecutor for parallel screenshot processing
    results_status = {}
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS_SCREENSHOT) as executor:
        futures = {
            executor.submit(take_screenshot_worker, url, path, item_id): item_id
            for url, path, item_id in screenshot_tasks
        }
        
        completed = 0
        for future in as_completed(futures):
            item_id = futures[future]
            try:
                success = future.result()
                results_status[item_id] = "success" if success else "failed"
                completed += 1
                status_symbol = "✓" if success else "✗"
                print(f"[{completed}/{len(screenshot_tasks)}] {item_id} {status_symbol}")
            except Exception as e:
                results_status[item_id] = "failed"
                completed += 1
                print(f"[{completed}/{len(screenshot_tasks)}] {item_id} ✗ (Exception: {str(e)[:50]})")
    
    # Update screenshot status in results
    for result in all_results:
        item_id = result.get("id", "unknown")
        if item_id in results_status:
            result["screenshot_status"] = results_status[item_id]
        else:
            result["screenshot_status"] = "skipped"


def main():
    # Load existing domains first
    load_seen_domains()
    
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

    # Filter duplicate domains
    new_domains_list = []
    filtered_no_duplicates = []
    
    for r in filtered:
        domain = extract_domain(r.get("href", "-"))
        if not is_domain_duplicate(domain):
            filtered_no_duplicates.append(r)
            new_domains_list.append(domain)
            add_domain_to_set(domain)
        else:
            print(f"[SKIP] Domain duplicate: {domain}")
    
    if not filtered_no_duplicates:
        print("Semua domain adalah duplikat. Tidak ada yang diproses.")
        return

    # Get last ID and start from next
    last_id = get_last_id()
    current_id = last_id + 1

    print(f"Starting ID: {current_id:08d}")
    print(f"Total URLs to process: {len(filtered_no_duplicates)}")

    # Process URLs with multithreading for fetching
    all_results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_FETCH) as executor:
        futures = {
            executor.submit(fetch_url_data, url_item, idx + 1, current_id + idx): idx
            for idx, url_item in enumerate(filtered_no_duplicates)
        }

        for future in as_completed(futures):
            result = future.result()
            all_results.append(result)

    # Sort by ID to maintain order
    all_results.sort(key=lambda x: int(x["id"]))

    # Process screenshots in parallel (multiprocessing)
    process_screenshots_parallel(all_results)

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

    # Save new domains to file
    save_new_domains(new_domains_list)

    # Count screenshot results
    screenshot_success = sum(1 for r in all_results if r.get("screenshot_status") == "success")
    screenshot_failed = sum(1 for r in all_results if r.get("screenshot_status") == "failed")
    screenshot_skipped = sum(1 for r in all_results if r.get("screenshot_status") == "skipped")

    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"Total records: {len(all_results)}")
    print(f"Total unique domains saved: {len(SEEN_DOMAINS)}")
    print(f"Screenshots - Success: {screenshot_success}, Failed: {screenshot_failed}, Skipped: {screenshot_skipped}")
    print(f"Generated at: {timestamp_iso}")
    print(f"Output file: {json_filepath}")
    print(f"Screenshots saved in: {OUTPUT_IMG_DIR}")
    print(f"All domains tracked in: {ALL_DOMAINS_FILE}")


if __name__ == "__main__":
    main()
