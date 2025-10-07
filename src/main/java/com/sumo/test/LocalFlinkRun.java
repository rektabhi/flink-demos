package com.sumo.test;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LocalFlinkRun {

    public static void main(String[] args) throws Exception {
        // 1. Get the local execution environment
        // Flink automatically detects it's being run from an IDE
        // and starts a local environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Create a data stream from a simple collection
        DataStream<String> textStream = env.fromElements(
                "hello flink",
                "local development is easy",
                "hello world"
        );

        // 3. Define the data processing logic
        DataStream<String> processedStream = textStream
                .map(String::toUpperCase) // Convert each string to uppercase
                .filter(line -> line.startsWith("HELLO")); // Filter for lines starting with "HELLO"

        // 4. Print the results to the console
        // This is a "sink" that writes the output.
        processedStream.print();

        // 5. Execute the job
        // This call is what triggers the actual execution.
        env.execute("My First Local Flink Job");
    }
}
