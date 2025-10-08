package com.sumo.fraud;

import com.sumo.fraud.core.RollingAverageFunction;
import com.sumo.fraud.model.Transaction;
import com.sumo.fraud.model.UserAverage;
import com.sumo.fraud.source.TransactionSource;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.concurrent.TimeUnit;

public class TransactionAggregator {

    private static final Integer NUM_USERS = 1;
    private static final long TTL = TimeUnit.SECONDS.toMillis(3L * NUM_USERS);

    public static void main(String[] args) throws Exception {
        // 1. Set up the Beam pipeline options
//        PipelineOptions options = PipelineOptionsFactory.create();
        DirectOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DirectOptions.class);
        options.setTargetParallelism(1);
        options.setRunner(DirectRunner.class); // Explicitly set the runner

        Pipeline pipeline = Pipeline.create(options);

        // 2. Create an unlimited transaction source
        PCollection<Transaction> transactionStream = pipeline
                .apply("Generate Unlimited Transactions", new TransactionSource(NUM_USERS));

        // 3. Print transactions
        transactionStream.apply("Print Transactions", ParDo.of(new DoFn<Transaction, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("Transaction: " + c.element());
            }
        }));

        // 4. Process the stream - apply rolling average directly
        PCollection<UserAverage> averageStream = transactionStream
                .apply("Apply Rolling Average", ParDo.of(new RollingAverageFunction(TTL)));

        // 5. Print the results to the console
        averageStream.apply("Print Averages", ParDo.of(new DoFn<UserAverage, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("User Average: " + c.element());
            }
        }));

        // 6. Execute the pipeline
        pipeline.run().waitUntilFinish();
    }
}