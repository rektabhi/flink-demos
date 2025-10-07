package com.sumo.fraud.core;

import com.sumo.fraud.model.Transaction;
import com.sumo.fraud.model.UserAverage;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.List;

public class RollingAverageFunction extends DoFn<Transaction, UserAverage> {

    private final long ttl;
    private final List<Transaction> transactionList = new ArrayList<>();
    private double currentSum = 0.0;

    public RollingAverageFunction(long ttl) {
        this.ttl = ttl;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Transaction transaction = c.element();
        String userId = transaction.getUserId();

        // Add the new transaction
        transactionList.add(transaction);
        currentSum += transaction.getAmount();

        // Clean up expired transactions
        long currentTime = System.currentTimeMillis();
        List<Transaction> validTransactions = new ArrayList<>();
        double validSum = 0.0;

        for (Transaction tx : transactionList) {
            if (currentTime - tx.getTimestamp() <= ttl) {
                validTransactions.add(tx);
                validSum += tx.getAmount();
            } else {
                System.out.println("Expiring transaction = " + tx.getTimestamp() + " with value " + tx.getAmount());
            }
        }

        // Update state
        transactionList.clear();
        transactionList.addAll(validTransactions);
        currentSum = validSum;

        // Calculate and emit the new average
        if (!transactionList.isEmpty()) {
            double average = currentSum / transactionList.size();
            c.output(new UserAverage(userId, average));
        }
    }
}

