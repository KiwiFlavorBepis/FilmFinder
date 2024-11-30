import requests
from bs4 import BeautifulSoup

def scrape_imdb_plot_summary(imdb_id):
    """
    Scrape plot summary from IMDB using the movie's IMDB ID
    
    Args:
        imdb_id (str): IMDB ID of the movie
    
    Returns:
        str: Plot summary text, or None if not found
    """
    url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"
    
    try:
        # Send a request with a user agent to avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the list item with a specific ID pattern
        plot_li = soup.find('li', id=lambda x: x and x.startswith('po'))
        
        if plot_li:
            # Navigate through the nested divs to find the plot summary text
            plot_div = plot_li.find('div', class_='ipc-html-content-inner-div', recursive=True)
            
            if plot_div:
                return plot_div.get_text(strip=True)
        
        return None
    
    except requests.RequestException as e:
        print(f"Error fetching IMDB page: {e}")
        return None
    
def scrape_first_tagline(imdb_id):
    """
    Scrape the first tagline for a movie from IMDB
    
    Args:
        imdb_id (str): IMDB ID of the movie
    
    Returns:
        str: First tagline, or None if not found
    """
    url = f"https://www.imdb.com/title/{imdb_id}/taglines/"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the specific UL with the given class
        tagline_ul = soup.find('ul', class_='ipc-metadata-list ipc-metadata-list--dividers-between sc-bda8bbe6-0 jxZlgE meta-data-list-full ipc-metadata-list--base')
        
        if tagline_ul:
            # Get the first LI and extract its text
            first_tagline_li = tagline_ul.find('li', class_='ipc-metadata-list__item')
            
            if first_tagline_li:
                tagline_div = first_tagline_li.find('div', class_='ipc-html-content-inner-div')
                return tagline_div.get_text(strip=True) if tagline_div else None
        
        return None
    
    except requests.RequestException as e:
        print(f"Error fetching IMDB taglines: {e}")
        return None
    
def scrape_keywords(imdb_id):
    """
    Scrape keywords for a movie from IMDB
    
    Args:
        imdb_id (str): IMDB ID of the movie
    
    Returns:
        list: List of keywords, or empty list if not found
    """
    url = f"https://www.imdb.com/title/{imdb_id}/keywords/"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the specific UL with the given class
        keywords_ul = soup.find('ul', class_='ipc-metadata-list ipc-metadata-list--dividers-after sc-bda8bbe6-0 jxZlgE ipc-metadata-list--base')
        
        keywords = []
        if keywords_ul:
            # Find all LI elements with the metadata item class
            keyword_items = keywords_ul.find_all('li', class_='ipc-metadata-list-summary-item', limit=15)
            
            for keyword_li in keyword_items:
                keyword_div = keyword_li.find('a', class_='ipc-metadata-list-summary-item__t')
                if keyword_div:
                    keyword = keyword_div.get_text(strip=True)
                    keywords.append(keyword)
    except requests.RequestException as e:
        print(f"Error fetching IMDB keywords: {e}")
        return []
        
    return ', '.join(keywords) if keywords else None
    
def scrape_genres(imdb_id):
    """
    Scrape genres from the main IMDB page for a movie
    
    Args:
        imdb_id (str): IMDB ID of the movie
    
    Returns:
        str: Comma-separated genres, or None if not found
    """
    url = f"https://www.imdb.com/title/{imdb_id}/"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all divs with the chip list scroller class
        chip_list_scrollers = soup.find_all('div', class_='ipc-chip-list__scroller')
        
        # Take the first scroller (which should be genres)
        if chip_list_scrollers:
            genres_scroller = chip_list_scrollers[0]
            
            # Extract genre texts
            genres = [
                genre.find('span', class_='ipc-chip__text').get_text(strip=True) 
                for genre in genres_scroller.find_all('a', class_='ipc-chip')
            ]
            
            # Combine genres into a comma-separated string
            return ', '.join(genres) if genres else None
        
        return None
    
    except requests.RequestException as e:
        print(f"Error fetching IMDB page: {e}")
        return None

# Example usage
imdb_id = 'tt1375666'  # Inception's IMDB ID

plot_summary = scrape_imdb_plot_summary(imdb_id)
print(plot_summary)

first_tagline = scrape_first_tagline(imdb_id)
print(first_tagline)

genres = scrape_genres(imdb_id)
print(genres)

keywords = scrape_keywords(imdb_id)
print(keywords)