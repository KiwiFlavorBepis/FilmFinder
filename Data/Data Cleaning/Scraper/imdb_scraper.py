import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

#Const
SUCCESS_CODE = 200

#Helper functions

def getPageData(imdb_id):
    #Get Response
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/80.0.3987.162 Safari/537.36'
    }
    url = f"https://www.imdb.com/title/{imdb_id}/"
    response = requests.get(url, headers=header)
    
    #Check for success
    if response.status_code != SUCCESS_CODE:
        print(f"Error: Status code {response.status_code} for {imdb_id}")
        return None
    
    #Return parsed HTML data
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

def collectRuntime(soup):
    runtime_element = soup.find('li', attrs={'data-testid': 'title-techspec_runtime'})
    if runtime_element:
        runtime_text = runtime_element.find('div', class_='ipc-metadata-list-item__content-container').get_text(strip=True)
        if runtime_text:
            return runtime_text
    else:
        print("    Runtime: Not found")

def collectGenre(soup):
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
            return genres_str
        else:
            print("    Genres: Not found")
    else:
        print("    Genres element not found")

def collectLanguages(soup): 
    languages_element = soup.find('li', {'data-testid': 'title-details-languages'})
    languages_list = []
    
    if languages_element:
        language_links = languages_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
        
        if language_links:
            for language_link in language_links:
                language = language_link.get_text(strip=True)
                languages_list.append(language)
            
            languages_str = ', '.join(languages_list)
            return languages_str
        else:
            print("    Languages not found")
    else:
        print("    Languages element not found")


def collectOverview(soup):
    overview_element = soup.find('div', {'data-testid': 'storyline-plot-summary'})
    if overview_element:
        # Find the inner div that contains the actual text
        inner_div = overview_element.find('div', class_='ipc-html-content-inner-div')
        if inner_div:
            # Get text but remove the "—Warner Bros. Pictures" part
            overview_text = inner_div.get_text(strip=True)
            # Split on "—" and take the first part
            overview_text = overview_text.split('—')[0].strip()
            return overview_text
        else:
            print("    Overview inner div not found")
    else:
        print("    Overview element not found")

def collectTaglines(soup):
    taglines_element = soup.find('li', {'data-testid': 'storyline-taglines'})
    if taglines_element:
        tagline_span = taglines_element.find('span', class_='ipc-metadata-list-item__list-content-item')
        if tagline_span:
            tagline_text = tagline_span.get_text(strip=True)
            return tagline_text
            #print(f"    New Tagline: {tagline_text}")
        else:
            print("    Tagline span not found")
    else:
        print("    Taglines element not found")

def collectProductionCompany(soup):
    companies_element = soup.find('li', {'data-testid': 'title-details-companies'})
    companies_list = []
    
    if companies_element:
        company_links = companies_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
        
        if company_links:
            for company_link in company_links:
                company = company_link.get_text(strip=True)
                companies_list.append(company)
            
            companies_str = ', '.join(companies_list)
            return companies_str
            #print(f"    New Production Companies: {companies_str}")
        else:
            print("    Production companies links not found")
    else:
        print("    Production companies element not found")

def collectProductionCountries(soup):
    countries_element = soup.find('li', {'data-testid': 'title-details-origin'})
    countries_list = []
    
    if countries_element:
        country_links = countries_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
        
        if country_links:
            for country_link in country_links:
                country = country_link.get_text(strip=True)
                countries_list.append(country)
            
            countries_str = ', '.join(countries_list)
            return countries_str
            print(f"    New Production Countries: {countries_str}")
        else:
            print("    Production countries links not found")
    else:
        print("    Production countries element not found")

#Scrape an id
def scrape_imdb_id(imdb_id):
    
    soup = getPageData(imdb_id)
    if (soup == None):
        print("Failed to get page data")
        exit

    #init movie data dict
    movie_data = {}

    try:
        # Scraping for movie runtime
        runtime = collectRuntime(soup)
        if (runtime != None):
            movie_data['runtime'] = runtime
        
        # Scraping genres
        genres = collectGenre(soup)
        if (genres != None):
            movie_data['genres'] = genres

        # Scraping languages
        languages = collectLanguages(soup)
        if (languages != None):
            movie_data['spoken_languages'] = languages

        # Scraping overview/plot summary
        overview = collectOverview(soup)
        if (overview != None):
            movie_data['overview'] = overview

        # Scraping taglines
        tagline = collectTaglines(soup)
        if (tagline != None):
            movie_data['tagline'] = tagline

        # Scraping production companies
        productionCompanies = collectProductionCompany(soup)
        if (productionCompanies != None):
            movie_data['production_companies'] = productionCompanies

        # Scraping production countries
        countries = collectProductionCountries
        if (countries != None):
            movie_data['production_countries'] = countries
        
        return movie_data
        
    except Exception as e:
        print(f"Error for movie {imdb_id}: {str(e)}")
        return None
    
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
            
            movie_data = scrape_imdb_id(imdb_id)
        
        print("-" * 50)
        
        counter += 1
        if limit and counter >= limit:
            break
    
    df.to_csv(output_file, index=False)
    print(f"Processed {counter} movies")
    print(f"Data saved to output file")

# Example usage
if __name__ == "__main__":
    fileLoc = "Data\Data Cleaning\Scraper\cleanedData.csv"
    update_movie_dataset(fileLoc, 'updated_movie_dataset.csv', 50)