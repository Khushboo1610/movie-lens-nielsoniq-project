import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, regexp_extract, col, avg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Starting MovieLens Average Ratings job")

try:
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MovieLens Average Ratings") \
        .getOrCreate()

    logging.info("Spark session created")

    movies_path = "resources/movies.dat"
    ratings_path = "resources/ratings.dat"
    users_path = "resources/users.dat"

    #Read users data and Filter users aged between 18–49 
    users_df = spark.read.text(users_path)

    users_df = users_df.select(
        split(users_df.value, "::").getItem(0).alias("UserID"),
        split(users_df.value, "::").getItem(2).alias("Age"))

    users_df = users_df.filter((users_df.Age>=18) & (users_df.Age<=49))

    logging.info("data read and parsed.. Filtered users aged between 18–49")

    # Read movies data
    movies_df = spark.read.text(movies_path)
    movies_df = movies_df.select(
        split(movies_df.value, "::").getItem(0).alias("MovieID"),
        split(movies_df.value, "::").getItem(1).alias("Title"),
        split(movies_df.value, "::").getItem(2).alias("Genres")
    )
    logging.info("movies_df data read and parsed")

    ##DataQuality Checks applied on movies dataframe as the grouplens Readme file mentions that this file may have inconsistent and duplicate data##
    # 1. Filter out movies without valid year or null genres
    # 2. Dropping duplicate MovieId
    cleaned_movies_df = movies_df.filter(
        (col("Title").rlike(r"\(\d{4}\)")) &
        (~col("Genres").isNull()) &
        (col("Genres") != "")
    )
    cleaned_movies_df = cleaned_movies_df.dropDuplicates(["MovieID"])

    logging.info("Movies data cleaned (valid year and non-null genres, duplicates removed)")

    # Extract year from title
    year_movies_df = cleaned_movies_df.withColumn("Year", \
                        regexp_extract(col("Title"), r"\((\d{4})\)", 1) \
                        .cast("int")) \
                        .filter(col("Year") > 1989)

    logging.info("Year extracted and movies after 1989 filtered")

    # Explode genres
    final_movies_df = year_movies_df.withColumn("Genre", \
            explode(split(col("Genres"), "\\|"))) \
            .drop("Genres")

    logging.info("Genres exploded into individual rows")

    # Read ratings data
    ratings_df = spark.read.text(ratings_path)
    ratings_df = ratings_df.select(
        split(ratings_df.value, "::").getItem(0).alias("UserID"),
        split(ratings_df.value, "::").getItem(1).alias("MovieID"),
        split(ratings_df.value, "::").getItem(2).alias("Rating")
    )

    logging.info("ratings_df data read and parsed")

    #Joining the three dataframes
    joined_df = ratings_df.join(users_df, "UserID", "inner") \
                .join(final_movies_df, "MovieID", "inner")

    logging.info("DataFrames joined on MovieID and UserID")

    # Group by Year and Genre, compute average rating
    result_df = joined_df.groupBy("Year", "Genre") \
                .agg(avg("Rating").alias("AvgRating")) \
                .orderBy("Year", "Genre")

    logging.info("Average ratings calculated by Year and Genre")

    # Saving the result in two ways:
    # 1. Writing all the data to a single csv file for reading purpose only

    logging.info("Writing final result to one CSV file")
    result_df.coalesce(1).write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv("output/avg_ratings")
    
    # 2. Assuming for big data coming, will keep this Partitioning strategy on Year and Genre
    logging.info("Writing final result to CSV partitioned by Year and Genre")
    result_df.write \
            .option("header", "true") \
            .mode("overwrite") \
            .partitionBy("Year", "Genre") \
            .csv("output/avg_ratings_partitioned")
    
except Exception as e:
        logging.error(f"An error occurred during processing: {e}", exc_info=True)

spark.stop()
logging.info("Spark session stopped. Job complete.")
