import argparse
import os
import time
import random
import logging
import csv
import json
import pandas as pd
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import threading
import re

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scrape conference data from Civilica website.')
    
    parser.add_argument('--input', type=str, default='./data/conferences_merged_full.csv',
                       help='Input CSV file with conference IDs')
    parser.add_argument('--output', type=str, default='./data/civilica_parallel_output.csv',
                       help='Output CSV file for results')
    parser.add_argument('--failed', type=str, default='failed_urls.csv',
                       help='CSV file to log failed URLs')
    parser.add_argument('--filtered', type=str, default='filtered_conference_ids.csv',
                       help='CSV file to save filtered conference IDs')
    parser.add_argument('--driver', type=str, default='C:\\Users\\Ali\\msedgedriver.exe',
                       help='Path to Edge WebDriver executable')
    parser.add_argument('--start', type=int, default=0,
                       help='Starting index for conference IDs to process')
    parser.add_argument('--end', type=int, default=None,
                       help='Ending index for conference IDs to process (None for all)')
    parser.add_argument('--workers', type=int, default=8,
                       help='Number of parallel workers')
    parser.add_argument('--headless', action='store_true',
                       help='Run browser in headless mode')
    parser.add_argument('--no-parallel', action='store_false', dest='parallel',
                       help='Disable parallel processing')
    parser.add_argument('--timeout', type=int, default=12,
                       help='Page load timeout in seconds')
    parser.add_argument('--retries', type=int, default=2,
                       help='Maximum number of retries for failed requests')
    parser.add_argument('--save-every', type=int, default=100,
                       help='Save partial results after this many rows')
    parser.add_argument('--min-delay', type=float, default=0.1,
                       help='Minimum delay between requests')
    parser.add_argument('--max-delay', type=float, default=0.5,
                       help='Maximum delay between requests')
    
    return parser.parse_args()

# Global variables (will be set in main)
args = None
failed_urls = []
failed_urls_lock = threading.Lock()
result_rows = []
result_lock = threading.Lock()

def save_partial_results():
    with result_lock:
        if result_rows:
            file_exists = os.path.isfile(args.output)
            with open(args.output, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow([
                        'Conference_ID','Title','Link','Abstract','Citation',
                        'Authors','Conference_Name','Year','Keywords',
                        'View_Count','Page_Count','Authors_Map'
                    ])
                for row in result_rows:
                    writer.writerow(row)
            result_rows.clear()


def init_driver() -> webdriver.Chrome:
    options = ChromeOptions()
    if args.headless:
        options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('window-size=1920,1080')
    options.add_argument('user-agent=Mozilla/5.0')

    service = ChromeService()  # Assumes `chromedriver` is in PATH
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(args.timeout)
    return driver

def retry_get(driver: webdriver.Edge, url: str, cid: str = '') -> bool:
    for attempt in range(1, args.retries + 1):
        try:
            driver.get(url)
            return True
        except (TimeoutException, WebDriverException) as e:
            logging.warning('Attempt %d failed loading %s: %s', attempt, url, e)
            time.sleep(random.uniform(0.5, 1.0))
    with failed_urls_lock:
        failed_urls.append({'conference_id': cid, 'url': url})
    return False

def parse_article_list(driver, conference_id):
    articles = []
    try:
        ul = driver.find_element(By.ID, 'articleLists')
    except NoSuchElementException:
        return articles
    for li in ul.find_elements(By.TAG_NAME, 'li'):
        try:
            h2 = li.find_element(By.TAG_NAME, 'h2')
            a = h2.find_element(By.TAG_NAME, 'a')
            href = a.get_attribute('href')
            title = re.sub(r'^\d+\.\s*', '', a.text.strip())
            link = urljoin('https://civilica.com/', href)
            articles.append((conference_id, title, link))
        except NoSuchElementException:
            continue
    return articles

def extract_abstract(driver):
    try:
        div = driver.find_element(By.CSS_SELECTOR, 'div.prose.max-w-none.my-6.text-color-black.text-justify > div')
        return div.text.strip()
    except NoSuchElementException:
        return ''

def extract_citation(driver):
    try:
        bq = driver.find_element(By.CSS_SELECTOR, 'blockquote.container.mx-auto.mb-8')
        return bq.find_element(By.TAG_NAME, 'p').text.strip()
    except NoSuchElementException:
        return ''

def parse_citation_details(text):
    details = {'authors': '', 'conference': '', 'year': '', 'keywords': '', 'view_count': '0', 'page_count': ''}
    if not text:
        return details
    m = re.search(r'نوشته شده توسط(.*?)نویسنده مسئول', text)
    if m: details['authors'] = m.group(1).strip()
    m = re.search(r'کمیته علمی (.*?) پذیرفته شده است', text)
    if m: details['conference'] = m.group(1).strip()
    m = re.search(r'در سال (\d{4})', text)
    if m: details['year'] = m.group(1)
    if 'کلمات کلیدی' in text:
        part = text.split('کلمات کلیدی', 1)[-1]
        if 'هستند' in part: details['keywords'] = part.split('هستند')[0].strip()
    m = re.search(r'تاکنون (\d+) بار', text)
    if m: details['view_count'] = m.group(1)
    m = re.search(r'با (\d+) صفحه', text)
    if m: details['page_count'] = m.group(1)
    return details

def extract_authors_and_places(driver):
    authors_map = {}
    blocks = driver.find_elements(By.CSS_SELECTOR, 'div.my-2.flex.flex-row.items-center')
    for block in blocks:
        try:
            name = block.find_element(By.CSS_SELECTOR, 'div.flex.flex-col > a').text.strip()
        except NoSuchElementException:
            continue
        try:
            place = block.find_element(By.CSS_SELECTOR, 'div.flex.flex-col > p').text.strip()
        except NoSuchElementException:
            place = ''
        authors_map[name] = place
    return authors_map

def process_conference(conf_id):
    driver = init_driver()
    local_rows = []
    page = 1
    while True:
        url = f'https://civilica.com/l/{conf_id}/pgn-{page}/'
        if not retry_get(driver, url, cid=conf_id):
            break
        arts = parse_article_list(driver, conf_id)
        if not arts:
            break
        for _, title, link in arts:
            if retry_get(driver, link, cid=conf_id):
                abstract = extract_abstract(driver)
                citation = extract_citation(driver)
                details = parse_citation_details(citation)
                authors_map = extract_authors_and_places(driver)
                local_rows.append([
                    conf_id, title, link,
                    abstract, citation,
                    details['authors'], details['conference'],
                    details['year'], details['keywords'],
                    details['view_count'], details['page_count'],
                    json.dumps(authors_map, ensure_ascii=False)
                ])
                if len(local_rows) >= args.save_every:
                    with result_lock:
                        result_rows.extend(local_rows)
                    save_partial_results()
                    local_rows.clear()
                time.sleep(random.uniform(args.min_delay, args.max_delay))
        page += 1
        time.sleep(random.uniform(0.5, 1.0))
    driver.quit()

    if local_rows:
        with result_lock:
            result_rows.extend(local_rows)
        save_partial_results()

def main():
    global args
    args = parse_arguments()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    df = pd.read_csv(args.input)
    filtered = df[df['keywords'].notna() & (df['keywords'] != '')]
    filtered[['id', 'keywords']].to_csv(args.filtered, index=False)
    ids = filtered['id'].astype(str).tolist()
    subset = ids[args.start:args.end] if args.end else ids[args.start:]
    logging.info('Processing %d IDs (index %d to %s)', len(subset), args.start, args.end or 'end')

    if os.path.exists(args.output):
        os.remove(args.output)

    if args.parallel:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            pool.map(process_conference, subset)
    else:
        for cid in subset:
            process_conference(cid)

    # Save failed URLs
    if failed_urls:
        failed_df = pd.DataFrame(failed_urls)
        if os.path.exists(args.failed):
            existing_failed = pd.read_csv(args.failed)
            failed_df = pd.concat([existing_failed, failed_df]).drop_duplicates()
        failed_df.to_csv(args.failed, index=False, encoding='utf-8-sig')
        logging.warning('Some URLs failed and were saved/updated to %s', args.failed)

    logging.info('Done. Results in %s', args.output)

if __name__ == '__main__':
    main()

