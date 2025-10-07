package com.sumo.fraud.source;

import com.sumo.fraud.model.Transaction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionSource extends RichParallelSourceFunction<Transaction> {

    private volatile boolean running = true;
    private final Random random = new Random();
    private final String[] userIds;
    private long transactionCounter = 0;

    private static final Integer MIN_TXN_AMOUNT = 1;
    private static final Integer MAX_TXN_AMOUNT = 5;
    private static final Integer MIN_SLEEP_TIME = 1000;
    private static final Integer MAX_SLEEP_TIME = 2000;


    public TransactionSource(int n) {
        userIds = new String[n];
        for (int i = 1; i <= n; i++) {
            userIds[i - 1] = "user" + i;
        }
    }

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        System.out.println("Transaction source started - generating continuous stream of transactions...");

        while (running) {
            // Generate random transaction event
            String userId = userIds[random.nextInt(userIds.length)];
            ++transactionCounter;
            long transactionTime = Instant.now().toEpochMilli();
            int transactionAmount = ThreadLocalRandom.current().nextInt(MIN_TXN_AMOUNT, MAX_TXN_AMOUNT);
            Transaction event = new Transaction(userId, transactionAmount, transactionTime);

            // Emit the event
            ctx.collect(event);

            // Log every 10th transaction to show continuous generation
            if (transactionCounter % 10 == 0) {
                System.out.println("Generated " + transactionCounter + " transactions so far...");
            }

            // Sleep for a random interval for more consistent streaming
            Thread.sleep(ThreadLocalRandom.current().nextLong(MIN_SLEEP_TIME, MAX_SLEEP_TIME));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

