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
    private final int N = 1;

    public TransactionSource() {
        userIds = new String[N];
        for (int i = 1; i <= N; i++) {
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
            double transactionAmount = ThreadLocalRandom.current().nextDouble(1.0, 5.0);
            transactionAmount = (int) transactionAmount;
            Transaction event = new Transaction(userId, transactionAmount, transactionTime);

            // Emit the event
            ctx.collect(event);

            // Log every 10th transaction to show continuous generation
            if (transactionCounter % 10 == 0) {
                System.out.println("Generated " + transactionCounter + " transactions so far...");
            }

            // Sleep for a random interval between 200ms to 800ms for more consistent streaming
            Thread.sleep(ThreadLocalRandom.current().nextLong(1000, 2000));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

