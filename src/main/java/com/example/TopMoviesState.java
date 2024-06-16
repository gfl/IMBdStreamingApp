package com.example;

import scala.Serializable;

import java.util.List;

public class TopMoviesState implements Serializable {

    public TopMoviesState(List<Movie> movies) {
        this.topMovies = movies;
    }

    public List<Movie> getTopMovies() {
        return topMovies;
    }

    public void setTopMovies(List<Movie> topMovies) {
        this.topMovies = topMovies;
    }

    List<Movie> topMovies;


}