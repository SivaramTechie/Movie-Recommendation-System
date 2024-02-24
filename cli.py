from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc, year
from pyspark.sql.types import StringType

# Create SparkSession
spark = SparkSession.builder \
    .appName("GenreYearBasedMovieRecommendation") \
    .getOrCreate()

# Load ratings, movies, and tags data from Google Drive
ratings_df = spark.read.csv("./datasets/rating.csv", header=True, inferSchema=True)
movies_df = spark.read.csv("./datasets/movie.csv", header=True, inferSchema=True)
tags_df = spark.read.csv("./datasets/tag.csv", header=True, inferSchema=True)

# Preprocess data
ratings_df = ratings_df.select("userId", "movieId", "rating")
movies_df = movies_df.select("movieId", "title", "genres")
tags_df = tags_df.select("movieId", "tag", "timestamp")

# Split genres into individual genres
movies_df = movies_df.withColumn("genres", explode(split("genres", "\\|")))

# Extract year from timestamp
tags_df = tags_df.withColumn("year", year("timestamp"))

# Function to recommend movies based on genre, year, and high rating
def recommend_movies_by_genre_and_year(genre, year):
    recommended_movies = ratings_df.join(movies_df, on="movieId").filter(movies_df.genres == genre).join(tags_df, on="movieId").filter(tags_df.year == year).groupBy("title").agg({"rating": "avg"}).orderBy(desc("avg(rating)"))
    return recommended_movies

# Get user input for genre and year
selected_genre = input("Enter the genre you want to explore: ")
selected_year = int(input("Enter the year you are interested in: "))

# Show recommended movies for the selected genre and year
recommended_movies = recommend_movies_by_genre_and_year(selected_genre, selected_year)
recommended_movies.show()

# Stop SparkSession
spark.stop()
