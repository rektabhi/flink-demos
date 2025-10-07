package com.sumo.fraud.model;

public class UserAverage {


    private String userId;
    private double averageValue;

    public UserAverage() {
    }

    public UserAverage(String userId, double averageValue, long computationTime) {
        this.userId = userId;
        this.averageValue = averageValue;
    }

    @Override
    public String toString() {
        return ">> UserAverage{" + "userId='" + userId + '\'' + ", averageValue=" + averageValue + '}';
    }


}
