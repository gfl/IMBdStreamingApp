package com.example;

import java.io.Serializable;

public class Movie implements Serializable {

    private String tconst;
    private double averageRating;
    private int numVotes;
    private double ranking;

    public Movie() {}

    public Movie(String tconst, double averageRating, int numVotes, double ranking) {
        this.tconst = tconst;
        this.averageRating = averageRating;
        this.numVotes = numVotes;
        this.ranking = ranking;
    }

    public String getTconst() {
        return tconst;
    }

    public void setTconst(String tconst) {
        this.tconst = tconst;
    }

    public double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(double averageRating) {
        this.averageRating = averageRating;
    }

    public int getNumVotes() {
        return numVotes;
    }

    public void setNumVotes(int numVotes) {
        this.numVotes = numVotes;
    }

    public double getRanking() {
        return ranking;
    }

    public void setRanking(double ranking) {
        this.ranking = ranking;
    }
}

