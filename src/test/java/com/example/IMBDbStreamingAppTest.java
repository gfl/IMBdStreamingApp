package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.example.IMDbStreamingApp.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IMBDbStreamingAppTest {

    private SparkSession spark;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .appName("IMDbStreamingAppTest")
                .master("local[*]")
                .getOrCreate();
    }

    private Dataset<Row> createTitleRatingsDataset(List<Quartet<String, Double, Integer, Timestamp>> data) {
        List<Row> rows = data.stream().map(record -> RowFactory.create(
                record.getValue0(), record.getValue1(), record.getValue2(), record.getValue3())).collect(Collectors.toList()
        );

        return spark.createDataFrame(rows, schema);
    }

    @Test
    public void testGetTopRatedMoviesFiltersOutMoviesWithoutEnoughVotes() {
        Dataset<Row> input = createTitleRatingsDataset(
                Arrays.asList(
                        Quartet.with("t1", 8.5, 100, java.sql.Timestamp.valueOf("2024-06-15 22:41:30")),
                        Quartet.with("t2", 5.6, 500, java.sql.Timestamp.valueOf("2024-06-15 22:41:30")),
                        Quartet.with("t3", 7.5, 1000, java.sql.Timestamp.valueOf("2024-06-15 22:41:30")),
                        Quartet.with("t4", 8.5, 10, java.sql.Timestamp.valueOf("2024-06-15 22:41:30"))
                )
        );

        List<Movie> result = getTopRatedMovies(input, 500, 3).collectAsList();

        assertEquals(result.size(), 2);
        List<String> ids = result.stream().map(Movie::getTconst).collect(Collectors.toList());
        assertTrue(ids.containsAll(Arrays.asList("t2", "t3")));

    }

    @Test
    public void testGetTopRatedMoviesSelectsTheTopRankedMovies() {
        Dataset<Row> input = createTitleRatingsDataset(
                Arrays.asList(
                        Quartet.with("t1", 8.5, 700, java.sql.Timestamp.valueOf("2024-06-15 22:40:00")),
                        Quartet.with("t2", 5.6, 2000, java.sql.Timestamp.valueOf("2024-06-15 22:41:00")),
                        Quartet.with("t3", 7.5, 500, java.sql.Timestamp.valueOf("2024-06-15 22:42:00")),
                        Quartet.with("t4", 8.5, 800, java.sql.Timestamp.valueOf("2024-06-15 22:43:00")),
                        Quartet.with("t5", 10.0, 450, java.sql.Timestamp.valueOf("2024-06-15 22:41:30"))
                )
        );

        StructType topRankedMoviesSchema = new StructType()
                .add("tconst", "string")
                .add("ranking", "double");

        List<Movie> topRatedMovies = getTopRatedMovies(input, 500, 3).collectAsList();
        assertEquals(topRatedMovies.size(), 3);
        List<Pair<String, Double>> idsWithRankings = topRatedMovies.
                stream()
                .map(movie -> Pair.with(movie.getTconst(),
                        BigDecimal.valueOf(movie.getRanking()).setScale(2, RoundingMode.DOWN).doubleValue()))
                .collect(Collectors.toList());
        assertEquals(Arrays.asList(Pair.with("t2", 11.2), Pair.with("t4", 6.8), Pair.with("t1", 5.94)),
                idsWithRankings);
    }

    @Test
    public void testCalculateMostCredited() {
        List<Movie> topRankedMovies = Arrays.asList(
                new Movie("t1", 7.8, 5000, 8.5),
                new Movie("t2", 7.6, 3500, 8.0),
                new Movie("t3", 7.5, 4000, 7.2),
                new Movie("t4", 6.5, 7000, 7.0),
                new Movie("t5", 6.2, 1500, 5.5)
        );
        List<Row> principalRows = Arrays.asList(
                RowFactory.create("t1", 1, "n1", "director", "director", "NA"),
                RowFactory.create("t1", 2, "n2", "producer", "producer", "NA"),
                RowFactory.create("t1", 3, "n3", "actor", "actor", "John Smith"),
                RowFactory.create("t2", 1, "n1", "director", "director", "NA"),
                RowFactory.create("t2", 2, "n4", "producer", "producer", "NA"),
                RowFactory.create("t2", 3, "n2", "actor", "actor", "Main character"),
                RowFactory.create("t3", 1, "n5", "director", "director", "NA"),
                RowFactory.create("t3", 2, "n2", "producer", "producer", "NA"),
                RowFactory.create("t3", 3, "n6", "actor", "actor", "Main character"),
                RowFactory.create("t4", 1, "n3", "director", "director", "NA"),
                RowFactory.create("t4", 2, "n1", "producer", "producer", "NA")
        );
        Dataset<Row> result = calculateMostCredited(spark.createDataFrame(principalRows, titlePrincipalsSchema),
                spark.createDataset(topRankedMovies, Encoders.bean(Movie.class)), 3);

        assertEquals(3, result.count());
        assertEquals(Arrays.asList(RowFactory.create("n1", 3), RowFactory.create("n2", 3), RowFactory.create("n3", 2)),
                result.collectAsList());

    }

}
