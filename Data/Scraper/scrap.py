import pandas as pd
import warnings
from bs4 import BeautifulSoup
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import time
import random
import re
import os

def parse_runtime_to_minutes(runtime_text):
    hours_match = re.search(r'(\d+)hours?', runtime_text)
    minutes_match = re.search(r'(\d+)minutes?', runtime_text)

    total_minutes = 0
    if hours_match:
        total_minutes += int(hours_match.group(1)) * 60
    if minutes_match:
        total_minutes += int(minutes_match.group(1))

    return total_minutes if total_minutes > 0 else None


def create_retry_session(retries=5, backoff_factor=0.5, status_forcelist=(500, 502, 504, 429, 403), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def scrape_keywords(imdb_id):
    """Scrape keywords for a given IMDb ID."""
    url = f"https://www.imdb.com/title/{imdb_id}/keywords/"
    session = create_retry_session()
    time.sleep(random.uniform(1, 5))  # Random delay to avoid rate limiting

    try:
        response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if response.status_code != 200:
            print(f"Failed to retrieve keywords for {imdb_id}: Status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        keywords_ul = soup.find('ul', class_=lambda x: x and 'ipc-metadata-list' in x)
        if keywords_ul:
            keyword_items = keywords_ul.find_all('li', class_=lambda x: x and 'ipc-metadata-list-summary-item' in x, limit=15)
            keywords = [item.get_text(strip=True) for item in keyword_items]
            return ', '.join(keywords) if keywords else None
    except Exception as e:
        print(f"Error scraping keywords for {imdb_id}: {e}")
        return None


def update_keywords_column(input_file, output_dir, start_line=0, end_line=None, batch_size=1000):
    """
    Update the keywords column in the DataFrame with data scraped from IMDb.

    :param input_file: Path to the input CSV file
    :param output_dir: Directory to save the output files
    :param start_line: Start index for processing
    :param end_line: End index for processing (inclusive)
    :param batch_size: Number of updates after which to save a file
    """
    # Load the DataFrame
    df = pd.read_csv(input_file)
    end_line = end_line or len(df) - 1
    df_subset = df.iloc[start_line:end_line + 1]

    print(f"Processing {len(df_subset)} rows from line {start_line} to {end_line}")

    update_count = 0
    batch_start = start_line

    for index, row in df_subset.iterrows():
        if pd.isnull(row['keywords']):  # Only update if keywords are missing
            imdb_id = row['imdb_id']
            print(f"Scraping keywords for index {index}, IMDb ID: {imdb_id}")
            keywords = scrape_keywords(imdb_id)
            if keywords:
                df.at[index, 'keywords'] = keywords
                update_count += 1
                print(f"Updated keywords for index {index}: {keywords}")
            else:
                print(f"No keywords found for index {index}")

        # Save progress every `batch_size` updates
        if update_count > 0 and update_count % batch_size == 0:
            batch_end = index
            output_file = os.path.join(output_dir, f'scraped_{batch_start}_{batch_end}.csv')
            df.to_csv(output_file, index=False)
            print(f"Saved progress to {output_file}")
            batch_start = index + 1  # Update batch start

    # Save any remaining updates after the loop
    if update_count % batch_size != 0:
        batch_end = end_line
        output_file = os.path.join(output_dir, f'scraped_{batch_start}_{batch_end}.csv')
        df.to_csv(output_file, index=False)
        print(f"Saved final progress to {output_file}")


# Example usage
if __name__ == "__main__":
    input_file = '../Datasets/clean.csv'
    output_dir = '../Datasets/Scraped_Chunks'
    os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists
    start_line = 100
    end_line = 2000  # Inclusive

    start_time = time.time()
    update_keywords_column(input_file, output_dir, start_line=start_line, end_line=end_line, batch_size=1000)
    print(f"Process completed in {time.time() - start_time:.2f} seconds")
