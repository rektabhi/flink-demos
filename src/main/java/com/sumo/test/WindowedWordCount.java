package com.sumo.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.io.File;
import java.time.Duration;
import java.time.Instant;

public class WindowedWordCount {

    public static void main(String[] args) throws Exception {
        // 1. Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        File file = new File(WindowedWordCount.class.getClassLoader().getResource("input.csv").getFile());

        // 2. Read the text file source

        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(file.getAbsolutePath())).build();
        DataStream<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "File Source");

        // 3. Extract timestamps and generate watermarks
        // This step is crucial for event-time processing. It tells Flink how to
        // get the timestamp from each record and allows for 2 seconds of lateness.
        DataStream<String> linesWithTimestamps = lines.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((line, previousTimestamp) -> {
                            // CSV format: "timestamp,text"
                            String timestampStr = line.split(",")[0];
                            return Instant.parse(timestampStr).toEpochMilli();
                        })
        );

        // 4. Process the data: parse, tokenize, and map to (word, 1)
        DataStream<Tuple2<String, Integer>> pairs = linesWithTimestamps.flatMap(new Tokenizer());

        // 5. Key by word, create a 10-second window, and sum the counts
        DataStream<Tuple2<String, Integer>> wordCounts = pairs
                .keyBy(value -> value.f0) // Group by the word (field 0 of the tuple)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10))) // Define a 10-second tumbling window
                .sum(1); // Sum the counts (field 1 of the tuple)

        // 6. Print the results to the console
        wordCounts.print();

        // 7. Execute the Flink job
        env.execute("Windowed Word Count from CSV");
    }

    /**
     * A FlatMap function that splits a line of text into words
     * and emits a (word, 1) tuple for each word.
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Extract the text part after the comma
            String text = value.split(",", 2)[1];

            // Normalize and split the line into words
            String[] words = text.toLowerCase().split("\\W+");

            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}