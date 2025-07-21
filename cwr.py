#!/usr/bin/env python3
import os
import time
import random
import logging
import csv
import json
import pandas as pd
import re
import asyncio
import aiohttp
import argparse
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import requests
import nest_asyncio
nest_asyncio.apply()

# Default configuration
DEFAULT_INPUT_CSV = './data/conferences_merged_full.csv'
DEFAULT_FILTERED_CSV = 'filtered_conference_ids.csv'
OUTPUT_CSV_PREFIX = 'civilica_optimized_output'
FAILED_URLS_LOG_PREFIX = 'failed_urls'
MAX_WORKERS = 2
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
REQUEST_DELAY = (1.2, 2.5)
SAVE_EVERY = 500
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Scrape Civilica conference data')
    parser.add_argument('--start', type=int, required=True, help='Start index for conference processing')
    parser.add_argument('--end', type=int, required=True, help='End index for conference processing')
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT_CSV, 
                       help=f'Input CSV file (default: {DEFAULT_INPUT_CSV})')
    parser.add_argument('--filtered', type=str, default=DEFAULT_FILTERED_CSV,
                       help=f'Filtered output CSV (default: {DEFAULT_FILTERED_CSV})')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS,
                       help=f'Number of concurrent workers (default: {MAX_WORKERS})')
    return parser.parse_args()

def setup_logging():
    """Configure logging format and level"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('civilica_scraper.log')
        ]
    )

def create_session():
    """Create resilient HTTP session"""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update(HEADERS)
    return session

class CivilicaScraper:
    def __init__(self, args):
        self.args = args
        self.output_csv = f"{OUTPUT_CSV_PREFIX}_{args.start}_{args.end}.csv"
        self.failed_urls_log = f"{FAILED_URLS_LOG_PREFIX}_{args.start}_{args.end}.csv"
        self.failed_urls = []
        self.result_rows = []
        self.processed_count = 0
        self.start_time = time.time()
        
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        logging.info(f"Initialized scraper with start={args.start}, end={args.end}")
        logging.info(f"Output will be saved to: {self.output_csv}")
        logging.info(f"Failed URLs will be logged to: {self.failed_urls_log}")

    def save_results(self):
        """Save results to CSV"""
        if not self.result_rows:
            return
        
        file_exists = os.path.isfile(self.output_csv)
        with open(self.output_csv, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    'Conference_ID', 'Title', 'Link', 'Abstract', 'Citation',
                    'Authors', 'Conference_Name', 'Year', 'Keywords',
                    'View_Count', 'Page_Count', 'Authors_Map'
                ])
            writer.writerows(self.result_rows)
        self.result_rows.clear()
        logging.info(f"Saved {len(self.result_rows)} records to {self.output_csv}")

    def parse_article_list(self, html, conference_id):
        """Parse article list from HTML"""
        soup = BeautifulSoup(html, 'lxml')
        articles = []
        ul = soup.find('ul', id='articleLists')
        if not ul:
            return articles
        
        for li in ul.find_all('li'):
            h2 = li.find('h2')
            if not h2:
                continue
            a_tag = h2.find('a')
            if a_tag and a_tag.get('href'):
                title = re.sub(r'^\d+\.\s*', '', a_tag.text.strip())
                link = urljoin('https://civilica.com/', a_tag['href'])
                articles.append((conference_id, title, link))
        return articles

    def extract_keywords_from_page(self, html):
        """Extract keywords from article page using the new method"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Locate the container div using class attributes
        container = soup.find('div', class_=lambda x: x and 'text-color-base' in x and 'pt-2' in x and 'p-4' in x and 'my-4' in x and 'bg-white' in x and 'border' in x and 'rounded' in x)
        
        if not container:
            return None
        
        # Exclude the title paragraph and extract keywords from anchor tags
        keyword_anchors = container.select('div')
        keywords = [a.get_text(strip=True) for a in keyword_anchors if a.get_text(strip=True)]
        
        return ", ".join(keywords) if keywords else None

    def parse_article_page(self, html):
        """Parse article details from HTML"""
        soup = BeautifulSoup(html, 'lxml')
        
        # Abstract
        abstract_div = soup.select_one('div.prose.max-w-none.my-6.text-color-black.text-justify > div')
        abstract = abstract_div.text.strip() if abstract_div else ''
        
        # Citation
        citation_block = soup.select_one('blockquote.container.mx-auto.mb-8')
        citation = citation_block.find('p').text.strip() if citation_block else ''
        
        # Authors
        authors_map = {}
        author_blocks = soup.select('div.my-2.flex.flex-row.items-center')
        for block in author_blocks:
            name_tag = block.select_one('div.flex.flex-col > a')
            place_tag = block.select_one('div.flex.flex-col > p')
            if name_tag:
                name = name_tag.text.strip()
                place = place_tag.text.strip() if place_tag else ''
                authors_map[name] = place
        
        # View count
        view_count = '0'
        view_tag = soup.find('span', class_='text-color-muted')
        if view_tag:
            view_text = view_tag.text.strip()
            match = re.search(r'(\d+)', view_text)
            if match:
                view_count = match.group(1)
        
        # Keywords - using the new method
        keywords = self.extract_keywords_from_page(html)
        
        return {
            'abstract': abstract,
            'citation': citation,
            'authors': ', '.join(authors_map.keys()),
            'conference': '',
            'year': '',
            'keywords': keywords if keywords else '',
            'view_count': view_count,
            'page_count': '',
            'authors_map': authors_map
        }

    async def process_article(self, session, conference_id, title, link):
        """Process single article asynchronously"""
        try:
            async with session.get(link, timeout=REQUEST_TIMEOUT) as response:
                if response.status != 200:
                    raise Exception(f"Status {response.status}")
                html = await response.text()
                details = self.parse_article_page(html)
                
                self.processed_count += 1
                if self.processed_count % 10 == 0:
                    elapsed = time.time() - self.start_time
                    logging.info(f"Processed {self.processed_count} articles in {elapsed:.2f} seconds")
                
                return [
                    conference_id, title, link,
                    details['abstract'], details['citation'],
                    details['authors'], details['conference'],
                    details['year'], details['keywords'],
                    details['view_count'], details['page_count'],
                    json.dumps(details['authors_map'], ensure_ascii=False)
                ]
        except Exception as e:
            logging.error(f"Article failed: {link} - {str(e)}")
            self.failed_urls.append({'conference_id': conference_id, 'url': link, 'error': str(e)})
            return None

    async def process_conference(self, session, conference_id):
        """Process all articles in a conference"""
        page = 1
        while True:
            url = f'https://civilica.com/l/{conference_id}/pgn-{page}/'
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                    if response.status != 200:
                        break
                    html = await response.text()
                    articles = self.parse_article_list(html, conference_id)
                    
                    if not articles:
                        break
                    
                    tasks = []
                    for _, title, link in articles:
                        tasks.append(self.process_article(session, conference_id, title, link))
                        await asyncio.sleep(random.uniform(*REQUEST_DELAY))
                    
                    results = await asyncio.gather(*tasks)
                    for result in results:
                        if result:
                            self.result_rows.append(result)
                    
                    if len(self.result_rows) >= SAVE_EVERY:
                        self.save_results()
                    
                    page += 1
            except Exception as e:
                logging.error(f"Conference page failed: {url} - {str(e)}")
                self.failed_urls.append({'conference_id': conference_id, 'url': url, 'error': str(e)})
                break

    async def run(self):
        """Main scraping process"""
        # Load conference IDs
        df = pd.read_csv(self.args.input)
        filtered = df[df['keywords'].notna() & (df['keywords'] != '')]
        filtered[['id', 'keywords']].to_csv(self.args.filtered, index=False)
        ids = filtered['id'].astype(str).tolist()[self.args.start:self.args.end]
        
        logging.info(f'Processing {len(ids)} conferences from index {self.args.start} to {self.args.end}')
        
        # Create output file with header
        with open(self.output_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Conference_ID', 'Title', 'Link', 'Abstract', 'Citation',
                'Authors', 'Conference_Name', 'Year', 'Keywords',
                'View_Count', 'Page_Count', 'Authors_Map'
            ])
        
        # Process conferences in parallel
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            tasks = [self.process_conference(session, cid) for cid in ids]
            await asyncio.gather(*tasks)
        
        # Save remaining results
        self.save_results()
        
        # Save failed URLs
        if self.failed_urls:
            pd.DataFrame(self.failed_urls).to_csv(self.failed_urls_log, index=False)
            logging.info(f"Saved {len(self.failed_urls)} failed URLs to {self.failed_urls_log}")
        
        elapsed = time.time() - self.start_time
        logging.info(f'Scraping completed in {elapsed:.2f} seconds')
        logging.info(f'Processed {self.processed_count} articles total')
        logging.info(f'Results saved to {self.output_csv}')

def main():
    setup_logging()
    args = parse_arguments()
    
    if args.start < 0 or args.end <= args.start:
        logging.error("Invalid start/end values. End must be greater than start, and start must be >= 0")
        return
    
    scraper = CivilicaScraper(args)
    
    # Handle event loop properly
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(scraper.run())
        else:
            loop.run_until_complete(scraper.run())
    except RuntimeError as e:
        if "no running event loop" in str(e):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(scraper.run())
        else:
            raise

if __name__ == '__main__':
    main()
