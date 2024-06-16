package com.example;

import scala.Serializable;

import java.util.ArrayList;
import java.util.List;

public class TopMoviesState implements Serializable {

    List<Movie> topMovies;
    Integer countMovies;
    Integer numberOfVotes;

    public TopMoviesState() {
        this.topMovies = new ArrayList<>();
        this.countMovies = 0;
        this.numberOfVotes = 0;
    }

    public TopMoviesState(List<Movie> movies) {
        this.topMovies = movies;
        this.countMovies = 0;
        this.numberOfVotes = 0;
    }

    public TopMoviesState(List<Movie> movies, Integer countMovies, Integer numberOfVotes) {
        this.topMovies = new ArrayList<>();
        this.countMovies = countMovies;
        this.numberOfVotes = numberOfVotes;
    }

    public List<Movie> getTopMovies() {
        return topMovies;
    }

    public void setTopMovies(List<Movie> topMovies) {
        this.topMovies = topMovies;
    }

    public Integer getCountMovies() {
        return countMovies;
    }

    public void setCountMovies(Integer countMovies) {
        this.countMovies = countMovies;
    }

    public Integer getNumberOfVotes() {
        return numberOfVotes;
    }

    public void setNumberOfVotes(Integer numberOfVotes) {
        this.numberOfVotes = numberOfVotes;
    }


}