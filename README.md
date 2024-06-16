# IMBD Streaming App

## Purpose

Spark streaming application that provides details of the top 10 movies in IMDB with a minimum ranking.

## Requirements

- Java 8+
- Apache Maven 3.6.0+
- Apache Spark 3.5.1+
- IMBDb datasets (https://datasets.imdbws.com/)

## Testing

To run the tests, use the following command:

```shell
mvn clean test
```

## Building

To build the project run

```shell
mvn clean package
```

## Execution

To run the application, use the following command:

```shell
export SPARK_LOCAL_IP="127.0.0.1"
spark-submit --class com.example.IMDbStreamingApp --master local[*] target/IMDbStreamingApp-1.0-SNAPSHOT-jar-with-dependencies.jar /path/to/imbdb/files
```
