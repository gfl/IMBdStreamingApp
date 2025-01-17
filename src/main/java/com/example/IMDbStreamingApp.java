package com.example;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class IMDbStreamingApp {

    public static StructType schema = new StructType()
            .add("tconst", "string")
            .add("averageRating", "double")
            .add("numVotes", "integer")
            .add("timestamp", "timestamp");

    public static StructType titlePrincipalsSchema = new StructType()
            .add("tconst", "string")
            .add("ordering", "integer")
            .add("nconst", "string")
            .add("category", "string")
            .add("job", "string")
            .add("characters", "string");

    public static StructType titleAkasSchema = new StructType()
            .add("titleId", "string")
            .add("ordering", "int")
            .add("title", "string")
            .add("region", "string")
            .add("language", "string")
            .add("types", "string")
            .add("attributes", "string")
            .add("isOriginalTitle", "int");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: IMDbStreamingApp <input-path>");
            System.exit(1);
        }

        String inputPath = args[0];

        // Set up the Spark session
        SparkSession spark = SparkSession.builder()
                .appName("IMDbStreamingApp")
                .getOrCreate();

        StructType titleRatingsSchema = new StructType()
                .add("tconst", "string")
                .add("averageRating", "double")
                .add("numVotes", "integer");

        Dataset<Row> titlesWithRatings = spark.readStream().option("sep", "\t").schema(titleRatingsSchema)
                .csv(inputPath + "title.ratings*.tsv")
                .withColumn("timestamp", current_timestamp())
                .withWatermark("timestamp", "1 minute");

        Dataset<Movie> topRatedMovies = getTopRatedMovies(titlesWithRatings, 500, 10);

        Dataset<Row> titleAkas = spark.readStream().option("sep", "\t").schema(titleAkasSchema)
                .csv(inputPath + "title.akas*.tsv")
                .withColumn("timestamp", current_timestamp())
                .withWatermark("timestamp", "1 minute");

//        Dataset<Row> titlePrincipals = spark.readStream().option("sep", "\t").schema(titlePrincipalsSchema)
//                .csv(inputPath + "title.principals*.tsv")
//                .withColumn("timestamp", current_timestamp())
//                .withWatermark("timestamp", "1 minute");

//        Dataset<Row> mostCreditedPersons = calculateMostCredited(titlePrincipals, topRatedMovies, 10);

        // Start the query to continuously display the top 10 movies
        StreamingQuery query1 = topRatedMovies.writeStream()
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

//        StreamingQuery query2 = mostCreditedPersons.writeStream()
//                .outputMode("append")
//                .format("console")
//                .trigger(Trigger.ProcessingTime("10 seconds"))
//                .start();

        // Await termination
        query1.awaitTermination();
//        query2.awaitTermination();
    }

    /**
     * Calculates the most credited people from the top 10 rated movies
     *
     * @param titlePrincipals Dataset with the top people involved in each movie
     * @param top10Movies Dataset with the top 10 rated movies
     * @param numberSelection number of people to select
     * @return Dataset with the most credited people from the top 10 movies
     */
    public static Dataset<Row> calculateMostCredited(Dataset<Row> titlePrincipals, Dataset<Movie> top10Movies, Integer numberSelection) {

        Dataset<String> top10MoviesIds = top10Movies.map((MapFunction<Movie, String>) Movie::getTconst, Encoders.STRING());
        Dataset<Row> top10Credits = top10MoviesIds.join(titlePrincipals, expr("value == tconst"));

        return top10Credits.groupBy("nconst")
                .count()
                .orderBy(col("count").desc())
                .limit(numberSelection);
    }

    /**
     * Returns top ranked movies that have a minimum number of ratings
     *
     * @param movies          Dataset with the IMBDb Movies, their avg rating and the number of votes
     * @param minVotes        minimum number of votes that the move should have to be kept
     * @param numberSelection number of movies to select
     * @return Dataset with the top movies that have at least minVotes votes.
     */
    public static Dataset<Movie> getTopRatedMovies(Dataset<Row> movies, Integer minVotes, Integer numberSelection) {

        Dataset<Row> filteredMovies = movies.filter("numVotes >= " + minVotes);
//        Dataset<Row> rankedMovies = calculateMovieRanking(filteredMovies);
        Dataset<Movie> movieDataset = filteredMovies.map((MapFunction<Row, Movie>) row ->
                new Movie(
                        row.getString(row.fieldIndex("tconst")),
                        row.getDouble(row.fieldIndex("averageRating")),
                        row.getInt(row.fieldIndex("numVotes"))
                ), Encoders.bean(Movie.class));

        // Define a mapGroupsWithState function to keep track of the top 10 movies
        final FlatMapGroupsWithStateFunction<String, Movie, TopMoviesState, Movie> top10MoviesFunc = (key, values, state) -> {
            TopMoviesState newState = state.exists() ? state.get() : new TopMoviesState(new ArrayList<>());
            List<Movie> topMovies = newState.getTopMovies();
            Integer counter = newState.getCountMovies();
            Integer numberOfVotes = newState.getNumberOfVotes();
            while(values.hasNext()) {
                Movie movie = values.next();
                topMovies.add(movie);
                numberOfVotes = numberOfVotes + movie.getNumVotes();
                counter++;
            }
            newState.setCountMovies(counter);
            newState.setNumberOfVotes(numberOfVotes);
            double avgNumVotes = (double) newState.getNumberOfVotes() / newState.getCountMovies();
            topMovies.forEach(movie -> movie.setRanking((movie.getNumVotes()/avgNumVotes) * movie.getAverageRating()));
            topMovies.sort(Comparator.comparingDouble(Movie::getRanking).reversed());
            if (topMovies.size() > numberSelection) {
                topMovies = topMovies.subList(0, numberSelection);
            }
            newState.setTopMovies(topMovies);
            state.update(newState);
            return topMovies.iterator();
        };

        return movieDataset
                .as(Encoders.bean(Movie.class))
                .groupByKey((MapFunction<Movie, String>) movie -> "top10", Encoders.STRING())
                .flatMapGroupsWithState(
                        top10MoviesFunc,
                        OutputMode.Append(),
                        Encoders.bean(TopMoviesState.class),
                        Encoders.bean(Movie.class),
                        GroupStateTimeout.NoTimeout());

    }

}
