import datetime
import aiohttp
import asyncio
from bs4 import BeautifulSoup

# List of starting URLs
START_URLS = ["http://example.com", "http://wikipedia.com"]

# Queue for URLs to be crawled
url_queue = asyncio.Queue()
for url in START_URLS:
    url_queue.put_nowait(url)

# Set of already crawled URLs
crawled_urls = set()

# Define asynchronous fetch function
async def fetch(url, session):
    start_time = datetime.now()
    async with session.get(url) as response:
        response.elapsed()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        ip_address = response.remote
        # fetch geolocation for ip_address...
        return await response.text(), duration, ip_address

#lock.acquire
# file_write
#lock.release


# use ip address of servers with region to find geolocation

# Define asynchronous crawler function
async def crawl(session):
    counter = 0
    while not url_queue.empty() and counter < 100:

        # Get valid URL to crawl
        # - a valid URL is one that has not been crawled (required shared datastructure)

        url = await url_queue.get()
        if url not in crawled_urls:
            crawled_urls.add(url)

            # Start crawling if valid url

            print(f"Crawling: {url}")

            counter += 1

            try:
                html_content = await fetch(url, session)


                soup = BeautifulSoup(html_content, "html.parser")
                # Extract and queue new URLs
                for a_tag in soup.find_all("a", href=True):
                    abs_url = a_tag.attrs["href"]
                    if abs_url not in crawled_urls:


                        url_queue.put_nowait(abs_url)
            except Exception as e:
                print(f"Error crawling {url}: {e}")


# each parallel worker is a crawler

# within each crawler
# - Get valid URL to crawl
# - Fetch the URL
# - Store fetched content into database
#   - ip address, response time, geo-location, content 
#   - save the URL into a visited list
# - Insert only new URLs to crawling list (reference visited list and insert fresh links)

# end of crawling
# - compile all content pages into a single report



async def main():
    # Create a session and spawn multiple crawlers
    # threadpool executor?
    async with aiohttp.ClientSession() as session:
        tasks = [crawl(session) for _ in range(10)]  # 10 crawlers
        await asyncio.gather(*tasks)

# Execute the crawler
asyncio.run(main())