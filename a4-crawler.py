import sqlite3
import threading
import concurrent.futures
import requests
import socket
import json
import geocoder
import time
from queue import Queue
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from urllib.request import urlopen
from urllib import robotparser
from summariser import summarise
from newspaper import Article

# Pls run "pip install -r requirements.txt" before attempting to run this Python script.

# Initial URLs
START_URLS = ["https://kotaku.com/", "https://sg.yahoo.com/topics/sg-gaming/", "https://www.channelnewsasia.com/topic/gaming", "https://sea.ign.com/"]
IP_MAPPINGS = {}
IGNORE_URLS = ["youtube", "twitter"]
# note, this is an upper limit. this does not mean that we will visited exactly 4096 URLs
# this is because we use a naive counting method to determine if we might have reached
# that limit. Furthermore, we have the blocked urls. Approx 5minutes to run.
LIMIT = 4096
MAX_WORKERS = 256
SLEEP_TIME = 100

words = []
with open("./keywords.txt", 'r') as f:
    words = [word.strip() for word in f]

# Queue is thread safe, so we do not need to implement anything for it.
url_queue = Queue()
url_blocked = set()
url_history = set()
url_to_visit = set()
interesting_url = set()

# Create a lock to synchronize access to the database
db_lock = threading.Lock()
set_lock = threading.Lock()
crawl_lock = threading.Lock()
log_lock = threading.Lock()

start_time = 0
stop_event = threading.Event()
stop = False
stop_flag = False
def log_message():
    if (not stop_flag and url_queue.qsize() >= LIMIT):
        set_flag(True)
    print(f"Time elapsed: {time.time() - start_time} | Processed: {len(url_history) + len(url_blocked)} | Approximate items left in queue: {url_queue.qsize()}")

def log_worker():
    global stop
    while not stop:
        if (len(url_history) >= LIMIT):
            stop = True
            stop_event.set()
        # Log your messages here
        log_message()
        time.sleep(5)

def set_flag(cond):
    global stop_flag
    with set_lock:
        stop_flag = cond

def add_to_set(set, item):
    with set_lock:
        set.add(item)

def remove_from_set(set, item):
    with set_lock:
        set.remove(item)

rp = {}
for link in START_URLS:
    url_queue.put(link)
    add_to_set(url_to_visit, link)
    domain = urlparse(link).netloc
    rp[domain] = robotparser.RobotFileParser()
    rp[domain].set_url(f"https://{domain}/robots.txt")
    rp[domain].read()

# Inserts the relevant details into our database
def insert_data(url, response_time, ip_address, region):
    conn = sqlite3.connect('a4-table.db')
    cursor = conn.cursor()
    cursor.execute("INSERT INTO table_name (url, response_time, ip_address, region) VALUES (?, ?, ?, ?)",
                    (url, response_time, ip_address, region))
    conn.commit()
    conn.close()

def fetch_url(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    domain = urlparse(url).netloc
    ip_address = socket.gethostbyname(domain)
    geolocation = get_region_from_ip(ip_address)
    
    return response.content, response.elapsed.total_seconds(), ip_address, geolocation, response.status_code

# Note that geocoder limits us to 50k API calls per day.
def get_region_from_ip(ip):
    # Either call API that retrieves geolocation information from IP
    # or make use of an existing library that does that for us.
    if IP_MAPPINGS.get(ip) is None:
        g = geocoder.ip(ip)
        IP_MAPPINGS[ip] = g.city
    return IP_MAPPINGS[ip]


# Checks if the URL is crawlable. We use robotparser to adhere to the
# rules defined in robots.txt of each domain.
def crawlable(url):
    domain = urlparse(url).netloc
    
    # This makes use of the robotsparser.
    with crawl_lock:
        if domain in rp:
            if not rp[domain].can_fetch("*", url):
                add_to_set(url_blocked, url)
                return False
        else:
            # New domain, so we do not know yet.
            rp[domain] = robotparser.RobotFileParser()
            rp[domain].set_url(f"https://{domain}/robots.txt")
            rp[domain].read()
    
    return True


# Here, we make use of newspaper3k to get the HTML content of the website in
# text format.
def read_content(url):
    article = Article(url)
    article.download()
    article.parse()
    
    title = article.title
    text = article.text
    title = title.encode('ascii', 'ignore').decode('ascii')
    text = text.encode('ascii', 'ignore').decode('ascii')
    
    return title, text.lower()

# This is an alternative, manual (and naive) way of getting the HTML content in text format.
# This only takes <p> tags, and then we manually process it.
def is_candidate_url(url):
    page = urlopen(url)
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string

    paragraphs = soup.find_all('p')
    paragraphs = [p for p in paragraphs if "advertisement" not in p.text]
    # Most likely not an article, so we can filter it out
    if (len(paragraphs) < 10 or len(paragraphs) > 100):
        return None
    else:
        return title, paragraphs

# Strips the HTML tags from the <p> tags. This includes stripping of embedded tags
# We will use this only if newspaper3k is not allowed.
def parse_tags(tags):
    article = ''
    for sentence in tags:
        s = ''.join(ET.fromstring(str(sentence)).itertext())
        article += s
        
        # Clean article
        article = article.encode('ascii', 'ignore').decode('ascii')
    
    return article

def crawl(url):
    # Check if it can be visited
    for word in IGNORE_URLS:
        if word in url:
            add_to_set(url_blocked, url)
            return
    
    if not crawlable(url):
        # print(f"Unable to crawl {url} due to rules defined in robots.txt!")
        return
    
    time.sleep(0.1)
    try:
        content, response_time, ip_addr, geolocation, code = fetch_url(url)
    except Exception as e:
        return
    
    # Now that we have successfully visited the URL, 2 things to do:
    # 1) Add to our url history
    # 2) Add the information into our database
    for keyword in words:
        if keyword in url:
            add_to_set(interesting_url, url)
    add_to_set(url_history, url)
    with db_lock:
        insert_data(url, response_time, ip_addr, geolocation)
    
    # Find and update links that we can continue crawling
    with crawl_lock:
        update_links(url, content)

    

def generate_summary(url_list):
    confident_set = set()
    for url in url_list:
        try:
            title, article = read_content(url)
            # Summarise the article, then take 5 sentences with the
            # highest sentence strength.
            # Sentence strength is an indicator of the sentence's relevancy to that article.
            # If we can find the keyword in any of these 5 sentences, we can say with higher confidence
            # that this article is related to our scope, GAMING.
            summarised_article = summarise(article)
            for keyword in words:
                if keyword in summarised_article:
                    add_to_set(confident_set, url)
                    break
        except Exception as e:
            continue
    return confident_set

def update_links(url, content):
    soup = BeautifulSoup(content, "html.parser")
    
    base_url = urlparse(url)
    for a_tag in soup.find_all('a', href=True):
        link = a_tag['href']
        
        full_url = urljoin(base_url.geturl(), link)
        if (len(full_url)>120):
            # so that we can ignore it.
            add_to_set(url_history)
            continue
        # Check the URL can be linked to a domain
        if urlparse(full_url).netloc:
            # Check that this URL has not been visited
            if full_url in url_history:
                # print(f"'{full_url}' has been visited before")
                continue
            elif full_url in url_to_visit:
                # print(f"'{full_url}' will be visited in the future")
                continue
            elif not stop_flag:
                url_queue.put(full_url)
                add_to_set(url_to_visit, full_url)

def main():
    global start_time
    global stop
    start_time = time.time()
    reset_time = time.time()
    stuck_time = time.time()
    # Initialise DB
    with db_lock:
        conn = sqlite3.connect('a4-table.db')
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS table_name (url TEXT, response_time REAL, ip_address TEXT, region TEXT)")
        conn.commit()
        conn.close()
    
    # Call ThreadPoolExecutor to execute threads.
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = []
    while True:
        with log_lock:
            if (len(url_history) + len(url_blocked) >= LIMIT):
                break
            if (time.time() - reset_time > 10):
                log_message()
                reset_time = time.time()

        if not url_queue.empty():
            stuck_time = time.time() # reset the stuck timer
            curr_url = url_queue.get()
            remove_from_set(url_to_visit, curr_url)
            if curr_url in url_history:
                continue
            future = executor.submit(crawl, curr_url)
            futures.append(future)
        else:
            # Give a generous amount of time before breaking, because there might still be
            # many other links that are still being processed by other threads.
            if time.time() - stuck_time >= 120:
                print("likely stuck, breaking out of loop to end execution")
                break
            time.sleep(1)
            
    print("\n================= KILLING ALL FUTURE THREADS AND SHUTTING DOWN =================\n")
    executor.shutdown(wait=False)
    for future in futures:
        future.cancel()
    print("\n================= Threads have shut down! =================\n")
        
def end_routine():
    conv = list(interesting_url)
    print("performing end routine...")
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
    future = executor.submit(generate_summary, conv)
    result = future.result()
    executor.shutdown(wait=True)
    return result


# NOTE: -1 response time means that we have not visited it yet! it is just in
# our list of links that we have found through scraping.
def add_found_links():
    for link in url_to_visit:
        domain = urlparse(link).netloc
        ip_address = socket.gethostbyname(domain)
        geolocation = get_region_from_ip(ip_address)
        insert_data(link, -1, ip_address, geolocation)

main()
print(f"finished executing main(). sleeping {SLEEP_TIME}s to allow more threads to finish, before continuing...")
time.sleep(SLEEP_TIME)
cs = end_routine()

domain_map = {}

print("\n============ SUMMARY OF INTERESTING URLS ============\n")
for url in interesting_url:
    print(url)
    domain = urlparse(url).netloc
    if (domain_map.get(domain) is not None):
        domain_map[domain] = domain_map.get(domain) + 1
    else:
        domain_map[domain] = 1
print("\n============ END OF SUMMARY ============\n")

l1 = len(url_history)
l2 = len(interesting_url)

domains = domain_map.keys()

top_hit = ''
most_num = 0
for domain in domains:
    if (domain_map[domain] > most_num):
        top_hit = domain
        most_num = domain_map[domain]
num_blocked = len(url_blocked)

print("\n============ BASIC STATISTICS ============\n")
print(f"visited: {l1}\ninteresting: {l2}\npercentage of interesting (relevant) urls: {l2 / l1}")
print(f"top hit domain is {top_hit}, with a total of {most_num} relevant urls")
print(f"number of blocked sites (from robots.txt) {num_blocked}")
print(f"total urls visited (including blocked): {l1 + num_blocked}")
print(f"urls not yet visited: {len(url_to_visit)}")
if cs is not None:
    print(f"out of the interesting urls, highly confident that {len(cs)} are related to GAMING.")
print("\n============ END OF BASIC STATISTICS ============\n")

# These are for analysing and actually creating the report in PDF format.
with open("remaining.json", 'w') as rem_file:
    json.dump(list(url_to_visit), rem_file)

with open("history.json", 'w') as json_file:
    json.dump(list(url_history), json_file)

with open("interesting.json", 'w') as interesting_file:
    json.dump(list(interesting_url), interesting_file)

with open("confident.json", 'w') as confident_file:
    json.dump(list(cs), confident_file)

with open("blocked.json", 'w') as blocked_file:
    json.dump(list(url_blocked), blocked_file)

print("done dumping all files")
