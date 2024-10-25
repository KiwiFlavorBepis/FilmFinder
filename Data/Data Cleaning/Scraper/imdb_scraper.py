import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

def scrape_imdb_id(imdb_id):
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/80.0.3987.162 Safari/537.36'
    }
    url = f"https://www.imdb.com/title/{imdb_id}/"
    response = requests.get(url, headers=header)
    
    if response.status_code != 200:
        print(f"Error: Status code {response.status_code} for {imdb_id}")
        return None
    
    soup = BeautifulSoup(response.text, 'html.parser')
    movie_data = {}
    
    try:
        # Scraping for movie runtime
        runtime_element = soup.find('li', attrs={'data-testid': 'title-techspec_runtime'})
        if runtime_element:
            runtime_text = runtime_element.find('div', class_='ipc-metadata-list-item__content-container').get_text(strip=True)
            if runtime_text:
                movie_data['runtime'] = runtime_text
        else:
            print("    Runtime: Not found")
        
        # Scraping genres
        genres_element = soup.find('div', {'data-testid': 'genres'})
        if not genres_element:
            genres_element = soup.find('li', {'data-testid': 'storyline-genres'})
            
        genres_list = []
        
        if genres_element:
            # Try both possible class names for genre links
            genre_links = genres_element.find_all('a', class_=['ipc-chip__text', 
                'ipc-metadata-list-item__list-content-item'])
            
            if genre_links:
                for genre_link in genre_links:
                    genre = genre_link.get_text(strip=True)
                    genres_list.append(genre)
                
                genres_str = ', '.join(genres_list)
                movie_data['genres'] = genres_str
            else:
                print("    Genres: Not found")
        else:
            print("    Genres element not found")

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
            else:
                print("    Languages not found")
        else:
            print("    Languages element not found")

        # Scraping overview/plot summary
        overview_element = soup.find('div', {'data-testid': 'storyline-plot-summary'})
        if overview_element:
            # Find the inner div that contains the actual text
            inner_div = overview_element.find('div', class_='ipc-html-content-inner-div')
            if inner_div:
                # Get text but remove the "—Warner Bros. Pictures" part
                overview_text = inner_div.get_text(strip=True)
                # Split on "—" and take the first part
                overview_text = overview_text.split('—')[0].strip()
                movie_data['overview'] = overview_text
            else:
                print("    Overview inner div not found")
        else:
            print("    Overview element not found")

        # Scraping taglines
        taglines_element = soup.find('li', {'data-testid': 'storyline-taglines'})
        if taglines_element:
            tagline_span = taglines_element.find('span', class_='ipc-metadata-list-item__list-content-item')
            if tagline_span:
                tagline_text = tagline_span.get_text(strip=True)
                movie_data['tagline'] = tagline_text
                print(f"    New Tagline: {tagline_text}")
            else:
                print("    Tagline span not found")
        else:
            print("    Taglines element not found")

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

        # NEW CODE: Scraping production countries
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
        
        return movie_data
        
    except Exception as e:
        print(f"Error for movie {imdb_id}: {str(e)}")
        return None

# Main processing loop
def update_movie_dataset(input_file, output_file, limit=None):
    df = pd.read_csv(input_file)
    
    print("\n Scraping start")
    print(f"Total movies in dataset: {len(df)}")
    if limit:
        print(f"Scrape first {limit} movies")
    print("\n")
    
    counter = 0
    for index, row in df.iterrows():
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
        
        if (pd.isnull(row['genres']) or pd.isnull(row['runtime']) or 
            pd.isnull(row['spoken_languages']) or pd.isnull(row['overview']) or 
            pd.isnull(row['tagline']) or pd.isnull(row['production_companies']) or
            pd.isnull(row['production_countries'])):
            
            print("Scraping missing data from IMDb:")
            movie_data = scrape_imdb_id(imdb_id)
            
            if movie_data:
                # Update all available fields
                for field in ['runtime', 'genres', 'spoken_languages', 'overview', 'tagline', 
                            'production_companies', 'production_countries']:
                    if field in movie_data:
                        df.at[index, field] = movie_data[field]
                        print(f"    New {field}: {movie_data[field]}")
            else:
                print("    No new data found")
        else:
            print("Skipping - existing data found")
        
        print("-" * 50)
        
        counter += 1
        if limit and counter >= limit:
            break
    
    df.to_csv(output_file, index=False)
    print(f"Processed {counter} movies")
    print(f"Data saved to output file")

# Example usage
if __name__ == "__main__":
    update_movie_dataset('og_movie_dataset.csv', 'updated_movie_dataset.csv', limit=None)