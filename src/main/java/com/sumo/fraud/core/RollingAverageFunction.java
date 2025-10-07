package com.sumo.fraud.core;


import com.sumo.fraud.model.Transaction;
import com.sumo.fraud.model.UserAverage;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RollingAverageFunction extends KeyedProcessFunction<String, Transaction, UserAverage> {

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
    public void open(OpenContext openContext) {
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
        // TODO: Check if event time or processing time is more appropriate
        //            ctx.timerService().registerEventTimeTimer(cleanupTimestamp);
        ctx.timerService().registerProcessingTimeTimer(cleanupTimestamp);

        // 4. Calculate and emit the new average
        long count = 0L;
        for (Tuple2<Long, Double> ignored : transactionState.get()) {
            count++;
        }
        out.collect(new UserAverage(transaction.getUserId(), newSum / count));
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

