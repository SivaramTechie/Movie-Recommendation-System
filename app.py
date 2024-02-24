from flask import Flask, render_template, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc, year
from pyspark.sql.types import StringType

# Initialize Flask app
app = Flask(__name__)

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

# Define route for homepage
@app.route('/')
def index():
    return render_template('index.html')

# Define route for movie recommendation
@app.route('/recommend', methods=['POST'])
def recommend():
    if request.method == 'POST':
        selected_genre = request.form['genre']
        selected_year = int(request.form['year'])
        recommended_movies = recommend_movies_by_genre_and_year(selected_genre, selected_year)
        movie_list = recommended_movies.collect()
        return render_template('recommendation.html', movies=movie_list)

if __name__ == '__main__':
    app.run(debug=True)
