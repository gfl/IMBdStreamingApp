package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.example.IMDbStreamingApp.schema;
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

    private Dataset<Row> createTitleRatingsDataset(List<Triplet<String, Double, Integer>> data) {
        List<Row> rows = data.stream().map(record -> RowFactory.create(
                record.getValue0(), record.getValue1(), record.getValue2())).collect(Collectors.toList()
        );

        return spark.createDataFrame(rows, schema);
    }

    @Test
    public void testGetTopRatedMoviesFiltersOutMoviesWithoutEnoughVotes() {
        Dataset<Row> input = createTitleRatingsDataset(
                Arrays.asList(
                        Triplet.with("t1", 8.5, 100),
                        Triplet.with("t2", 5.6, 500),
                        Triplet.with("t3", 7.5, 1000),
                        Triplet.with("t4", 8.5, 10)
                )
        );

        List<Row> result = IMDbStreamingApp.getTopRatedMovies(spark, input, 500, 3)
                .collectAsList();

        assertEquals(result.size(), 2);
        List<String> titles = result.stream().map(row -> row.getString(0)).collect(Collectors.toList());
        assertTrue(titles.containsAll(Arrays.asList("t2", "t3")));

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
                        Triplet.with("t1", 5.6, 500),
                        Triplet.with("t2", 7.5, 1000)
                )
        );
        List<Pair<String, Double>> titlesWithRankings = extraMoviesAndRatings(
                IMDbStreamingApp.calculateMovieRanking(spark, input)
        );
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
                        Triplet.with("t1", 8.5, 700),
                        Triplet.with("t2", 5.6, 2000),
                        Triplet.with("t3", 7.5, 500),
                        Triplet.with("t4", 8.5, 800),
                        Triplet.with("t4", 10.0, 450)
                )
        );
        List<Pair<String, Double>> topRankedMovies = extraMoviesAndRatings(
                IMDbStreamingApp.getTopRatedMovies(spark, input, 500, 3)
        );
        assertEquals(topRankedMovies.size(), 3);
        assertEquals(topRankedMovies,
                Arrays.asList(
                        Pair.with("t2", 11.2),
                        Pair.with("t4", 6.8),
                        Pair.with("t1", 5.94))
        );
    }


}
