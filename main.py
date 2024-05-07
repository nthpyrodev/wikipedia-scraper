import requests
from bs4 import BeautifulSoup
import csv
import time
import re
from concurrent.futures import ThreadPoolExecutor
import threading
import multiprocessing

# Function to extract links from a Wikipedia page
def extract_links(url, session, cache):
    if url in cache and time.time() - cache[url]['timestamp'] < 30:
        # Return cached data if available and not expired
        return cache[url]['links']
    
    links = []
    try:
        response = session.get(url)
        response.raise_for_status()
        content = response.text
        soup = BeautifulSoup(content, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href.startswith('/wiki/') and ':' not in href and not re.search(r'\.(svg|jpg|jpeg|gif|png|tif|tiff)$', href):
                links.append(href)
    except Exception as e:
        print(f"Error scraping page {url}: {e}")
    
    # Update cache with new data and timestamp
    cache[url] = {'links': links, 'timestamp': time.time()}
    
    return links

# Function to scrape Wikipedia and log the data to a CSV file
def scrape_wikipedia(start_url):
    session = requests.Session()  # Create a session object for each process
    visited_links = {}  # Dictionary to store visited links for each process
    writer_lock = threading.Lock()  # Lock for synchronizing access to shared data
    cache = {}  # Cache for storing scraped data
    
    queue = [start_url]
    visited_combinations = set()  # Set to store visited combinations of 'From Page' and 'To Page'
    batch_size = 1000
    batch = []
    
    start_time = time.time()  # Start timing from the beginning
    
    with open('wikipedia_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['From Page', 'To Page']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        while queue:
            current_url = queue.pop(0)
            if current_url in visited_links:  # Skip if already visited
                continue
            visited_links[current_url] = set()  # Initialize set for visited links from this page
            
            try:
                links = extract_links('https://en.wikipedia.org' + current_url, session, cache)
            except Exception as e:
                print(f"Error extracting links from page {current_url}: {e}")
                continue
            
            links_scraped = len(links)  # Count the number of links scraped from this page
            for link in links:
                combination = (current_url, link)
                if combination in visited_combinations:  # Skip if combination already visited
                    continue
                visited_combinations.add(combination)
                batch.append({'From Page': current_url, 'To Page': link})
                visited_links[current_url].add(link)  # Add link to visited links
                if link not in visited_links:  # Add link to queue only if it's not already visited
                    queue.append(link)
            pages_scraped = len(visited_links)  # Count the number of pages scraped so far
            
            # Write batch to CSV and reset batch if batch size is reached
            if len(batch) >= batch_size:
                writer_lock.acquire()  # Acquire the lock before writing to CSV
                try:
                    writer.writerows(batch)
                finally:
                    writer_lock.release()  # Release the lock
                batch = []
            
            elapsed_time = time.time() - start_time  # Calculate elapsed time
            page_speed = pages_scraped / elapsed_time if elapsed_time > 0 else 0
            link_speed = links_scraped / elapsed_time if elapsed_time > 0 else 0
            print(f"Scraped {pages_scraped} pages and {links_scraped} links in {elapsed_time:.2f} seconds. Average speed: {page_speed:.2f} pages/second, {link_speed:.2f} links/second.")

        # Write any remaining links in the batch to CSV
        if batch:
            writer_lock.acquire()  # Acquire the lock before writing to CSV
            try:
                writer.writerows(batch)
            finally:
                writer_lock.release()  # Release the lock

if __name__ == '__main__':
    # List of start URLs for each instance of the scraper
    start_urls = ['/wiki/Wikipedia', '/wiki/Python_(programming_language)', '/wiki/Artificial_intelligence', '/wiki/Machine_learning']

    # Create a process for each start URL
    processes = []
    for start_url in start_urls:
        process = multiprocessing.Process(target=scrape_wikipedia, args=(start_url,))
        processes.append(process)
        process.start()

    # Wait for all processes to finish
    for process in processes:
        process.join()