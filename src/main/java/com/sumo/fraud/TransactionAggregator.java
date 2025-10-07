package com.sumo.fraud;

import com.sumo.fraud.core.RollingAverageFunction;
import com.sumo.fraud.model.Transaction;
import com.sumo.fraud.model.UserAverage;
import com.sumo.fraud.source.TransactionSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TransactionAggregator {

    private static final Integer NUM_USERS = 3;
    private static final long TTL = TimeUnit.SECONDS.toMillis(3L * NUM_USERS);

    public static void main(String[] args) throws Exception {
        // 1. Set up the Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1)) {

            // 2. Create a sample data source
            DataStream<Transaction> transactionStream = env.addSource(new TransactionSource(NUM_USERS)).name("transactions");
            transactionStream.print();

            // 3. Process the stream
            DataStream<UserAverage> averageStream = transactionStream
                    .keyBy(Transaction::getUserId)
                    .process(new RollingAverageFunction(TTL));

            // 4. Print the results to the console
            averageStream.print();

            // 5. Execute the job
            env.execute("User Rolling Average");
        }
    }

}