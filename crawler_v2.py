import requests
from bs4 import BeautifulSoup
import threading
import sqlite3

# Database setup
conn = sqlite3.connect('crawler_data.db')
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS web_data (
    url TEXT PRIMARY KEY,
    response_time FLOAT,
    ip_address TEXT,
    region TEXT
)
""")
conn.commit()
conn.close()

# Lock for thread-safe database operations
db_lock = threading.Lock()

# Initial URLs
START_URLS = ["http://example.com", "http://anotherexample.com"]

# Shared data structures
url_queue = threading.Queue()
for url in START_URLS:
    url_queue.put(url)
crawled_urls = set()

def fetch(url):
    response = requests.get(url)
    ip_address = response.raw._connection.sock.getpeername()[0]
    return response.content, response.elapsed.total_seconds(), ip_address

def crawl():
    while not url_queue.empty():
        url = url_queue.get()
        if url not in crawled_urls:
            crawled_urls.add(url)
            try:
                html_content, response_time, ip_address = fetch(url)
                # Here, you'd fetch region from an IP geolocation API...
                region = "TODO"
                # Store data in DB with thread safety
                with db_lock:
                    conn = sqlite3.connect('crawler_data.db')
                    cursor = conn.cursor()
                    cursor.execute("INSERT OR IGNORE INTO web_data (url, response_time, ip_address, region) VALUES (?, ?, ?, ?)", (url, response_time, ip_address, region))
                    conn.commit()
                    conn.close()
                soup = BeautifulSoup(html_content, "html.parser")
                # Extract and queue new URLs
                for a_tag in soup.find_all("a", href=True):
                    abs_url = a_tag.attrs["href"]
                    if abs_url not in crawled_urls:
                        url_queue.put(abs_url)
            except Exception as e:
                print(f"Error crawling {url}: {e}")

# Start multiple crawler threads
threads = []
for _ in range(10):  # Create 10 threads
    t = threading.Thread(target=crawl)
    t.start()
    threads.append(t)

# Wait for all threads to finish
for t in threads:
    t.join()

# Analyze results or further processing...