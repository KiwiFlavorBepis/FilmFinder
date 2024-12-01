from sklearn.cluster import KMeans
from scipy.spatial.distance import euclidean
import pandas as pd
import numpy as np


def find_similar_movies_by_genre(movie, df, kmeans, k=20):
    movie_cluster = kmeans.predict([movie])[0]
    clustered_movies = df[df['genre_cluster'] == movie_cluster]
    clustered_movies['distance'] = clustered_movies['combined_genres'].apply(
        lambda x: euclidean(movie, x)
    )
    similar_movies = clustered_movies.nsmallest(k, 'distance')
    return similar_movies[['title']]


def find_similar_movies_by_summary(query_point, df, kmeans, k=20):
    query_point = np.array(query_point, dtype=np.float64)
    query_cluster = kmeans.predict([query_point])[0]
    cluster_movies = df[df['summary_cluster'] == query_cluster]
    cluster_movies['distance'] = cluster_movies['embedded_overview'].apply(
        lambda x: euclidean(query_point, x)
    )
    similar_movies = cluster_movies.nsmallest(k, 'distance')
    return similar_movies[['title']]


def main():
    file_name = "embedded_summary.xlsx"
    df = pd.read_excel(file_name)

    num_movies = int(input("Enter the number of similar films you want: "))
    movie_name = input("Enter the name of the movie: ")
    if movie_name not in df['title'].values:
        print(f"Movie '{movie_name}' not found in the dataset!")

    search_by = input("What do you want to search by: Genre, Summary, Tagline, or Keywords?")

    kmeans = KMeans(n_clusters=num_movies + 1, random_state=42)
    if search_by.lower() == "genre":
        df['combined_genres'] = df['combined_genres'].apply(eval)
        genre_points = np.array(df['combined_genres'].tolist())

        df['genre_cluster'] = kmeans.fit_predict(genre_points)

        input_genres = df[df['title'] == movie_name].iloc[0]['combined_genres']
        similar_movies = find_similar_movies_by_genre(input_genres, df, kmeans)
        print(similar_movies)
    elif search_by.lower() == "summary":
        overview_points = np.array(df['embedded_overview'].tolist(), dtype=np.float64)

        df['summary_cluster'] = kmeans.fit_predict(overview_points)

        input_summary = df[df['title'] == movie_name].iloc[0]['embedded_overview']
        similar_movies = find_similar_movies_by_summary(input_summary, df, kmeans)
        print(similar_movies)
    # elif search_by.lower() == "tagline":
    """
    tagline_points = np.array(df['embedded_taglines'].tolist(), dtype=np.float64)

    df['tagline_cluster'] = kmeans.fit_predict(tagline_points)

    input_tagline = df[df['title'] == movie_name].iloc[0]['embedded_taglines']
    similar_movies = find_similar_movies_by_tagline(input_tagline, df, kmeans)
    """
    # elif search_by.lower() == "keywords":
    """
    df['combined_keywords'] = df['combined_keywords'].apply(eval)
    keyword_points = np.array(df['combined_keywords'].tolist())

    df['keyword_cluster'] = kmeans.fit_predict(keyword_points)

    input_keywords = df[df['title'] == movie_name].iloc[0]['combined_keywords']
    similar_movies = find_similar_movies_by_keywords(input_keywords, df, kmeans)
    """


if __name__ == "__main__":
    main()
