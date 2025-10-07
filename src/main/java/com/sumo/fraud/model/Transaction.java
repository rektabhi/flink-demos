package com.sumo.fraud.model;

public class Transaction {
    private String userId;
    private double amount;
    private long timestamp;

    public Transaction() {}
    public Transaction(String userId, double amount, long timestamp) {
        this.userId = userId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public String getUserId() { return userId; }
    public double getAmount() { return amount; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "Transaction{" + "userId='" + userId + '\'' + ", amount=" + amount + ", timestamp=" + timestamp + '}';
    }

}
