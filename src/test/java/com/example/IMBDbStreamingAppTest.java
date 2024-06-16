package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
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
import static org.apache.spark.sql.functions.col;
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

    private List<Pair<String, Double>> extraMoviesAndRatings(Dataset<Row> rankedMovies) {
        return rankedMovies.select(col("tconst"), col("ranking"))
                .collectAsList().stream()
                .map(row -> Pair.with(row.getString(0), BigDecimal.valueOf(row.getDouble(1)).setScale(2,
                        RoundingMode.DOWN).doubleValue())).collect(Collectors.toList());
    }

    @Test
    public void testCalculateMovieRanking() {
        Dataset<Row> input = createTitleRatingsDataset(
                Arrays.asList(
                        Quartet.with("t1", 5.6, 500, java.sql.Timestamp.valueOf("2024-06-15 22:40:00")),
                        Quartet.with("t2", 7.5, 1000, java.sql.Timestamp.valueOf("2024-06-15 22:41:00"))
                )
        );
        List<Pair<String, Double>> titlesWithRankings = extraMoviesAndRatings(calculateMovieRanking(input));
        assertTrue(titlesWithRankings.containsAll(
                Arrays.asList(
                        Pair.with("t1", 3.73),
                        Pair.with("t2", 10.0))
        ));

    }

    @Test
    public void testGetTopRatedMoviesSelectsTheTopRankedMovies() {
        Dataset<Row> input = createTitleRatingsDataset(
                Arrays.asList(
                        Quartet.with("t1", 8.5, 700, java.sql.Timestamp.valueOf("2024-06-15 22:40:00")),
                        Quartet.with("t2", 5.6, 2000, java.sql.Timestamp.valueOf("2024-06-15 22:41:00")),
                        Quartet.with("t3", 7.5, 500, java.sql.Timestamp.valueOf("2024-06-15 22:42:00")),
                        Quartet.with("t4", 8.5, 800, java.sql.Timestamp.valueOf("2024-06-15 22:43:00")),
                        Quartet.with("t4", 10.0, 450, java.sql.Timestamp.valueOf("2024-06-15 22:41:30"))
                )
        );
//        List<Pair<String, Double>> topRankedMovies = extraMoviesAndRatings(getTopRatedMovies(input, 500, 3));
        List<Movie> topRatedMovies = getTopRatedMovies(input, 500, 3).collectAsList();
        assertEquals(topRatedMovies.size(), 3);
//        assertEquals(Arrays.asList(Pair.with("t2", 11.2), Pair.with("t4", 6.8), Pair.with("t1", 5.94)),
//                topRankedMovies);
    }


}
