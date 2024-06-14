package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

public class IMDbStreamingApp {

    public static StructType schema = new StructType()
            .add("tconst", "string")
            .add("averageRating", "double")
            .add("numVotes", "integer");

    public static void main(String[] args) throws Exception {
        // Set up the Spark session
        SparkSession spark = SparkSession.builder()
                .appName("IMDbStreamingApp")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> titlesWithRatings = spark.readStream().option("sep", "\t").schema(schema).csv("title.ratings.tsv");

        Dataset<Row> filteredMovies = getTopRatedMovies(spark, titlesWithRatings, 500, 10);

        // Start the query to continuously display the top 10 movies
        StreamingQuery query = filteredMovies.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        // Await termination
        query.awaitTermination();
    }

    /**
     * Returns top movies that have a minimum number of ratings
     *
     * @param spark           Spark session
     * @param movies          Dataset with the IMBDb Movies, their avg rating and the number of votes
     * @param minVotes        minimum number of votes that the move should have to be kept
     * @param numberSelection number of movies to select
     * @return Dataset with the top movies that have at least minVotes votes.
     */
    public static Dataset<Row> getTopRatedMovies(SparkSession spark, Dataset<Row> movies, Integer minVotes, Integer numberSelection) {
        return movies.filter("numVotes >= " + minVotes);
    }

}
