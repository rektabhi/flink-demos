package com.sumo.fraud;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class TransactionAggregator {

    public static void main(String[] args) throws Exception {
        // 1. Set up the Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1)) {


            // 2. Create a sample data source
            DataStream<Transaction> transactionStream = env.addSource(new TransactionSource()).name("transactions");
            transactionStream.print();

            // 3. Process the stream
            DataStream<UserAverage> averageStream = transactionStream
                    .keyBy(Transaction::getUserId)
                    .process(new RollingAverageFunction());

            // 4. Print the results to the console
            averageStream.print();

            // 5. Execute the job
            env.execute("User 7-Day Rolling Average");
        }
    }

    /**
     * The core logic for calculating the rolling average.
     */
    public static class RollingAverageFunction extends KeyedProcessFunction<String, Transaction, UserAverage> {

        private static final long TTL = TimeUnit.SECONDS.toMillis(3);

        /**
         * State to store the running sum of transactions for the current user.
         */
        private transient ValueState<Double> sumState;

        /**
         * State to store all transactions that are currently within the 7-day window.
         * We store the timestamp and the value to know when to expire them.
         */
        private transient ListState<Tuple2<Long, Double>> transactionState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // Initialize state descriptors
            ValueStateDescriptor<Double> sumDescriptor = new ValueStateDescriptor<>("sumState", Double.class);
            sumState = getRuntimeContext().getState(sumDescriptor);
            ListStateDescriptor<Tuple2<Long, Double>> transactionsDescriptor = new ListStateDescriptor<>(
                    "transactionState",
                    TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {})
            );
            transactionState = getRuntimeContext().getListState(transactionsDescriptor);
        }

        @Override
        public void processElement(Transaction transaction, Context ctx, Collector<UserAverage> out) throws Exception {
            // 1. Retrieve current sum, defaulting to 0.0 if it's the first event
            Double currentSum = 0.0;
            if (sumState != null && sumState.value() != null) {
                currentSum = sumState.value();
            }

            // 2. Add the new transaction to our state
            transactionState.add(Tuple2.of(transaction.getTimestamp(), transaction.getAmount()));
            Double newSum = currentSum + transaction.getAmount();
            sumState.update(newSum);

            // 3. Register a timer to fire after this event.
            // This timer will trigger the cleanup logic in the onTimer() method.
            long cleanupTimestamp = transaction.getTimestamp() + TTL;
//            ctx.timerService().registerEventTimeTimer(cleanupTimestamp);
            ctx.timerService().registerProcessingTimeTimer(cleanupTimestamp);

            // 4. Calculate and emit the new average
            long count = 0L;
            for (Tuple2<Long, Double> t : transactionState.get()) {
                count++;
            }
            out.collect(new UserAverage(transaction.getUserId(), newSum / count, System.currentTimeMillis()));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserAverage> out) throws Exception {
            // The timer fired, meaning some transactions are now older than TTL.
            long expirationTime = timestamp - TTL;
            System.out.println("Timer being invoked");

            // Prepare a list to hold transactions that are NOT expired
            List<Tuple2<Long, Double>> keptTransactions = new ArrayList<>();
            double updatedSum = 0.0;

            // Iterate through all stored transactions for the current user
            for (Tuple2<Long, Double> tx : transactionState.get()) {
                if (tx.f0 <= expirationTime) {
                    System.out.println("Expiring transaction = " + tx.f0 + " with value " + tx.f1);
                    // This transaction is expired, do nothing (don't add it to the new sum or list)
                } else {
                    // This transaction is still valid, keep it
                    keptTransactions.add(tx);
                    updatedSum += tx.f1;
                }
            }

            // Update the state with the cleaned list and sum
            transactionState.update(keptTransactions);
            sumState.update(updatedSum);
        }
    }

    public static class TransactionSource extends RichParallelSourceFunction<Transaction> {

        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] userIds;
        private long transactionCounter = 0;
        private final int N = 1;

        TransactionSource() {
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

    // --- Data Model POJOs ---
    public static class Transaction {
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

    public static class UserAverage {
        private String userId;
        private double averageValue;

        public UserAverage() {}
        public UserAverage(String userId, double averageValue, long computationTime) {
            this.userId = userId;
            this.averageValue = averageValue;
        }

        @Override
        public String toString() {
            return ">> UserAverage{" + "userId='" + userId + '\'' + ", averageValue=" + averageValue + '}';
        }
    }

}