import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

def parse_runtime_to_minutes(runtime_text):
    """Convert runtime text to minutes."""
    hours_match = re.search(r'(\d+)hours?', runtime_text)
    minutes_match = re.search(r'(\d+)minutes?', runtime_text)
    
    total_minutes = 0
    if hours_match:
        total_minutes += int(hours_match.group(1)) * 60
    if minutes_match:
        total_minutes += int(minutes_match.group(1))
    
    return total_minutes if total_minutes > 0 else None

def scrape_imdb_id(imdb_id):
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/80.0.3987.162 Safari/537.36'
    }

    # scraping from main imdb url
    url = f"https://www.imdb.com/title/{imdb_id}/"
    response = requests.get(url , headers=header)
    
    if response.status_code != 200:
        print(f"Error: Status code {response.status_code} for {imdb_id}")
        return None
    
    soup = BeautifulSoup(response.text, 'html.parser')

    # scraping from plot summary url
    url_summary = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"
    response_summary = requests.get(url_summary , headers=header)
    
    if response_summary.status_code != 200:
        print(f"Error: Status code {response_summary.status_code} for {imdb_id} plot summary")
        return None
    
    soup_summary = BeautifulSoup(response_summary.text, 'html.parser')

    # scraping from plot tagline url
    url_tagline = f"https://www.imdb.com/title/{imdb_id}/taglines/"
    response_tagline = requests.get(url_tagline , headers=header)
    
    if response_tagline.status_code != 200:
        print(f"Error: Status code {response_tagline.status_code} for {imdb_id} tagline")
        return None
    
    soup_tagline = BeautifulSoup(response_tagline.text, 'html.parser')

    # scraping from keywords url
    url_keywords = f"https://www.imdb.com/title/{imdb_id}/keywords/"
    response_keywords = requests.get(url_keywords , headers=header)

    if response_keywords.status_code != 200:
        print(f"Error: Status code {response_keywords.status_code} for {imdb_id} tagline")
        return None
    
    soup_keywords = BeautifulSoup(response_keywords.text, 'html.parser')

    movie_data = {}
    
    #main IMDB URL
    try:
        # Scraping genres
        chip_list_scrollers = soup.find_all('div', class_='ipc-chip-list__scroller')
        if chip_list_scrollers:
            genres_scroller = chip_list_scrollers[0]
            genres = [
                genre.find('span', class_='ipc-chip__text').get_text(strip=True) 
                for genre in genres_scroller.find_all('a', class_='ipc-chip')
            ]
            if genres:
                movie_data['genres'] = ', '.join(genres)
                print(f"    New Genres: {', '.join(genres)}")
            else:
                print("    Genres not found")

        # Scraping for movie runtime
        runtime_element = soup.find('li', attrs={'data-testid': 'title-techspec_runtime'})
        if runtime_element:
            runtime_text = runtime_element.find('div', class_='ipc-metadata-list-item__content-container').get_text(strip=True)
            if runtime_text:
                runtime_minutes = parse_runtime_to_minutes(runtime_text)
                if runtime_minutes:
                    movie_data['runtime'] = runtime_minutes
                    print(f"    New Runtime: {runtime_minutes}")
        else:
            print("    Runtime: Not found")

        # [Rest of the code remains the same as in previous version]
        # Scraping languages
        languages_element = soup.find('li', {'data-testid': 'title-details-languages'})
        languages_list = []
        
        if languages_element:
            language_links = languages_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
            
            if language_links:
                for language_link in language_links:
                    language = language_link.get_text(strip=True)
                    languages_list.append(language)
                
                languages_str = ', '.join(languages_list)
                movie_data['spoken_languages'] = languages_str
                print(f"    New Languages: {languages_str}")
            else:
                print("    Languages not found")
        else:
            print("    Languages element not found")

        # Scraping production companies
        companies_element = soup.find('li', {'data-testid': 'title-details-companies'})
        companies_list = []
        
        if companies_element:
            company_links = companies_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
            
            if company_links:
                for company_link in company_links:
                    company = company_link.get_text(strip=True)
                    companies_list.append(company)
                
                companies_str = ', '.join(companies_list)
                movie_data['production_companies'] = companies_str
                print(f"    New Production Companies: {companies_str}")
            else:
                print("    Production companies links not found")
        else:
            print("    Production companies element not found")

        # Scraping production countries
        countries_element = soup.find('li', {'data-testid': 'title-details-origin'})
        countries_list = []
        
        if countries_element:
            country_links = countries_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
            
            if country_links:
                for country_link in country_links:
                    country = country_link.get_text(strip=True)
                    countries_list.append(country)
                
                countries_str = ', '.join(countries_list)
                movie_data['production_countries'] = countries_str
                print(f"    New Production Countries: {countries_str}")
            else:
                print("    Production countries links not found")
        else:
            print("    Production countries element not found")

        # Scraping plot summary
        # Find the list item with a specific ID pattern
        plot_li = soup_summary.find('li', id=lambda x: x and x.startswith('po'))
        
        if plot_li:
            # Navigate through the nested divs to find the plot summary text
            plot_div = plot_li.find('div', class_='ipc-html-content-inner-div', recursive=True)
            
            if plot_div:
                overview_text = plot_div.get_text(strip=True)
                movie_data['overview'] = overview_text
                print(f"    New Plot Overview: {overview_text}")
            else:
                print("    Plot overview not found")
        else:
            print("    Plot overview element not found")

        # Scraping plot tagline
        # Find the specific UL with the given class
        tagline_ul = soup_tagline.find('ul', class_='ipc-metadata-list ipc-metadata-list--dividers-between sc-bda8bbe6-0 jxZlgE meta-data-list-full ipc-metadata-list--base')
        
        if tagline_ul:
            # Get the first LI and extract its text
            first_tagline_li = tagline_ul.find('li', class_='ipc-metadata-list__item')
            
            if first_tagline_li:
                tagline_div = first_tagline_li.find('div', class_='ipc-html-content-inner-div')
                first_tagline_text = tagline_div.get_text(strip=True) if tagline_div else None
                movie_data['tagline'] = first_tagline_text
                print(f"    New Plot Tagline: {first_tagline_text}")
            else:
                print("    Plot tagline not found")
        else:
            print("    Plot tagline element not found")

        # Scraping keywords
        # Find the specific UL with the given class
        keywords_ul = soup_keywords.find('ul', class_='ipc-metadata-list ipc-metadata-list--dividers-after sc-bda8bbe6-0 jxZlgE ipc-metadata-list--base')
        
        keywords = []
        keywords_str = None
        if keywords_ul:
            # Find all LI elements with the metadata item class
            keyword_items = keywords_ul.find_all('li', class_='ipc-metadata-list-summary-item', limit=15)
            
            for keyword_li in keyword_items:
                keyword_div = keyword_li.find('a', class_='ipc-metadata-list-summary-item__t')
                if keyword_div:
                    keyword = keyword_div.get_text(strip=True)
                    keywords.append(keyword)
            if keywords:
                keywords_str = ', '.join(keywords)
                movie_data['keywords'] = keywords_str
                print(f"    New Plot Keywords: {keywords_str}")
            else:
                print("    Plot keywords not found")
        else:
            print("    Plot keyword element not found")


        return movie_data
        
    except Exception as e:
        print(f"Error for movie {imdb_id}: {str(e)}")
        return None
    
    
def update_movie_dataset(input_file, output_file, start_line=None, end_line=None):
    #scrapes movies from the [start_line]'th line and ends at the [end_line]'th line
    # Read the entire CSV file
    df = pd.read_csv(input_file)
    
    # Adjust start and end lines if not specified
    if start_line is None:
        start_line = 0
    if end_line is None:
        end_line = len(df)
    
    # Slice the DataFrame to the specified range
    df_subset = df.iloc[start_line:end_line]
    
    print("\n Scraping start")
    print(f"Total movies in dataset: {len(df_subset)}")
    print(f"Processing from line {start_line} to {end_line}")
    print("\n")
    
    counter = 0
    for index, row in df_subset.iterrows():
        imdb_id = row['imdb_id']
        
        print(f"\nMovie {counter + 1}:")
        print(f"Title: {row['title']}")
        print(f"IMDb ID: {imdb_id}")
        print(f"Runtime: {row['runtime']}")
        print(f"Genres: {row['genres']}")
        print(f"Languages: {row['spoken_languages']}")
        print(f"Overview: {row['overview'][:100]}..." if not pd.isnull(row['overview']) else "Current Overview: None")
        print(f"Tagline: {row['tagline']}" if not pd.isnull(row['tagline']) else "Current Tagline: None")
        print(f"Production Companies: {row['production_companies']}" if not pd.isnull(row['production_companies']) else "Current Production Companies: None")
        print(f"Production Countries: {row['production_countries']}" if not pd.isnull(row['production_countries']) else "Current Production Countries: None")
        print(f"Keywords: {row['keywords']}" if not pd.isnull(row['keywords']) else "Current Keywords: None")
        
        if (pd.isnull(row['genres']) or pd.isnull(row['runtime']) or 
            pd.isnull(row['spoken_languages']) or pd.isnull(row['overview']) or 
            pd.isnull(row['tagline']) or pd.isnull(row['production_companies']) or
            pd.isnull(row['production_countries']) or pd.isnull(row['keywords'])):
            
            movie_data = scrape_imdb_id(imdb_id)
            
            # Only update columns that are currently None/NaN
            if pd.isnull(row['genres']) and 'genres' in movie_data:
                df.at[index, 'genres'] = movie_data['genres']
            
            if pd.isnull(row['runtime']) and 'runtime' in movie_data:
                df.at[index, 'runtime'] = movie_data['runtime']
            
            if pd.isnull(row['spoken_languages']) and 'languages' in movie_data:
                df.at[index, 'spoken_languages'] = movie_data['languages']
            
            if pd.isnull(row['overview']) and 'plot_overview' in movie_data:
                df.at[index, 'overview'] = movie_data['plot_overview']
            
            if pd.isnull(row['tagline']) and 'plot_tagline' in movie_data:
                df.at[index, 'tagline'] = movie_data['plot_tagline']
            
            if pd.isnull(row['production_companies']) and 'production_companies' in movie_data:
                df.at[index, 'production_companies'] = movie_data['production_companies']
            
            if pd.isnull(row['production_countries']) and 'production_countries' in movie_data:
                df.at[index, 'production_countries'] = movie_data['production_countries']
            
            if pd.isnull(row['keywords']) and 'plot_keywords' in movie_data:
                df.at[index, 'keywords'] = movie_data['plot_keywords']
        
        print("-" * 50)
        
        counter += 1
    
    # Save only the processed subset
    df_subset.to_csv(output_file, index=False)
    print(f"Processed {counter} movies")
    print(f"Data saved to output file")

# Example usage
if __name__ == "__main__":
    #scrape_imdb_id("tt1606389") # The Vow (missing all)
    scrape_imdb_id("tt1375666") # Inception (missing none)
    update_movie_dataset('modern_feature_films.csv', 'updated_movie_dataset.csv', 48, 54)