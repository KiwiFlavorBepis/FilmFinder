from scipy.spatial.distance import euclidean
import pandas as pd


def find_similar_movies_by_genre(movie, df, k):
    movie_cluster = df.loc[df['title'] == movie, 'genre_cluster']
    clustered_movies = df[df['genre_cluster'] == movie_cluster]
    clustered_movies['distance'] = clustered_movies['combined_genres'].apply(
        lambda x: euclidean(movie, x)
    )
    similar_movies = clustered_movies.nsmallest(k, 'distance')
    return similar_movies[['title']]


def find_similar_movies_by_summary(movie, df, k):
    movie_cluster = df.loc[df['title'] == movie, 'summary_cluster']
    cluster_movies = df[df['summary_cluster'] == movie_cluster]
    cluster_movies['distance'] = cluster_movies['embedded_overview'].apply(
        lambda x: euclidean(movie, x)
    )
    similar_movies = cluster_movies.nsmallest(k, 'distance')
    return similar_movies[['title']]


def find_similar_movies_by_keywords(movie, df, k):
    movie_cluster = df.loc[df['title'] == movie, 'keyword_cluster']
    cluster_movies = df[df['keyword_cluster'] == movie_cluster]
    cluster_movies['distance'] = cluster_movies['combined_keywords'].apply(
        lambda x: euclidean(movie, x)
    )
    similar_movies = cluster_movies.nsmallest(k, 'distance')
    return similar_movies[['title']]


def main():
    file_name = "movie_database.xlsx"
    df = pd.read_excel(file_name)

    num_movies = int(input("Enter the number of similar films you want: "))
    movie_name = input("Enter the name of the movie: ")
    if movie_name not in df['title'].values:
        print(f"Movie '{movie_name}' not found in the dataset!")

    search_by = input("What do you want to search by: Genre, Summary, Tagline, or Keywords?")

    if search_by.lower() == "genre":
        similar_movies = find_similar_movies_by_genre(movie_name, df, num_movies)
        print(similar_movies)
    elif search_by.lower() == "summary":
        similar_movies = find_similar_movies_by_summary(movie_name, df, num_movies)
        print(similar_movies)
    elif search_by.lower() == "keywords":
        similar_movies = find_similar_movies_by_keywords(movie_name, df, num_movies)
        print(similar_movies)


if __name__ == "__main__":
    main()
