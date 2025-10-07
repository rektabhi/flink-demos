package com.sumo.fraud.source;

import com.sumo.fraud.model.Transaction;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionSource extends PTransform<PBegin, PCollection<Transaction>> {

    private final int numUsers;

    private static final Integer MIN_TXN_AMOUNT = 1;
    private static final Integer MAX_TXN_AMOUNT = 5;
    private static final Integer MIN_SLEEP_TIME = 1000;
    private static final Integer MAX_SLEEP_TIME = 2000;

    public TransactionSource(int numUsers) {
        this.numUsers = numUsers;
    }

    @Override
    public PCollection<Transaction> expand(PBegin input) {
        // Create a list of transactions
        List<Transaction> transactions = new ArrayList<>();
        String[] userIds = new String[numUsers];
        for (int i = 1; i <= numUsers; i++) {
            userIds[i - 1] = "user" + i;
        }

        Random random = new Random();
        // Generate a limited number of transactions for testing
        for (int i = 0; i < 50; i++) {
            String userId = userIds[random.nextInt(userIds.length)];
            long transactionTime = Instant.now().toEpochMilli();
            int transactionAmount = ThreadLocalRandom.current().nextInt(MIN_TXN_AMOUNT, MAX_TXN_AMOUNT);
            Transaction event = new Transaction(userId, transactionAmount, transactionTime);
            transactions.add(event);
        }

        return input.apply("Generate Transactions", Create.of(transactions));
    }
}

