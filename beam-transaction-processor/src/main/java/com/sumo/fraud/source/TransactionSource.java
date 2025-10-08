package com.sumo.fraud.source;

import com.sumo.fraud.model.Transaction;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionSource extends PTransform<PBegin, PCollection<Transaction>> {

    private final int numUsers;

    private static final Integer MIN_TXN_AMOUNT = 1;
    private static final Integer MAX_TXN_AMOUNT = 5;
    private static final Integer MIN_SLEEP_TIME = 100;
    private static final Integer MAX_SLEEP_TIME = 500;

    public TransactionSource(int numUsers) {
        this.numUsers = numUsers;
    }

    @Override
    public PCollection<Transaction> expand(PBegin input) {
        return input.apply("Generate Unbounded Sequence", GenerateSequence.from(0))
                .apply("Generate Transactions", ParDo.of(new TransactionGeneratorFn(numUsers)));
    }

    private static class TransactionGeneratorFn extends DoFn<Long, Transaction> {
        private final int numUsers;
        private String[] userIds;
        private Random random;

        public TransactionGeneratorFn(int numUsers) {
            this.numUsers = numUsers;
        }

        @DoFn.Setup
        public void setup() {
            userIds = new String[numUsers];
            for (int i = 1; i <= numUsers; i++) {
                userIds[i - 1] = "user" + i;
            }
            random = new Random();
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws InterruptedException {
            String userId = userIds[random.nextInt(userIds.length)];
            long transactionTime = Instant.now().toEpochMilli();
            double transactionAmount = ThreadLocalRandom.current().nextDouble(MIN_TXN_AMOUNT, MAX_TXN_AMOUNT);
            Transaction event = new Transaction(userId, transactionAmount, transactionTime);

            // Add some processing delay to simulate real-time behavior
            int sleepTime = ThreadLocalRandom.current().nextInt(MIN_SLEEP_TIME, MAX_SLEEP_TIME);
            Thread.sleep(sleepTime);

            c.output(event);
        }
    }
}
