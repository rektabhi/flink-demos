package org.apache.flink.playgrounds.ops.clickcount;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEvent;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventStatistics;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * A simple streaming job reading {@link ClickEvent}s from Kafka, counting events per 15 seconds and
 * writing the resulting {@link ClickEventStatistics} back to Kafka.
 *
 * <p> It can be run with or without checkpointing and with event time or processing time semantics.
 * </p>
 *
 * <p>The Job can be configured via the command line:</p>
 * * "--checkpointing": enables checkpointing
 * * "--event-time": use an event time window assigner
 * * "--backpressure": insert an operator that causes periodic backpressure
 * * "--input-topic": the name of the Kafka Topic to consume {@link ClickEvent}s from
 * * "--output-topic": the name of the Kafka Topic to produce {@link ClickEventStatistics} to
 * * "--bootstrap.servers": comma-separated list of Kafka brokers
 */
public class SimpleWindowCounterTest2 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        try (final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)) {
            env.setParallelism(1); // Set parallelism to 1 for simplicity

            DataStream<Event> inputStream = env.fromElements(
                    new Event("A", 1000L),
                    new Event("B", 2000L),
                    new Event("A", 3000L),
                    new Event("A", 4000L),

                    new Event("B", 5000L),
                    new Event("B", 6000L),
                    new Event("B", 7000L),
                    new Event("A", 8000L),
                    new Event("B", 9000L),

                    new Event("B", 10000L),
                    new Event("A", 11000L),
                    new Event("B", 12000L),
                    new Event("C", 13000L),

                    new Event("A", 20000L)
            );

            DataStream<Event> watermarkedStream = inputStream.assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Event>forMonotonousTimestamps()
                            .withTimestampAssigner((event, recordTimestamp) -> event.timestamp)
            );

            KeyedStream<Event, String> keyedStream = watermarkedStream.keyBy(event -> event.key);

            WindowedStream<Event, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5))); // 5-second tumbling window


            DataStream<Tuple2<String, Long>> counts = windowedStream.process(
                    new ProcessWindowFunction<Event, Tuple2<String, Long>, String, TimeWindow>() {
                        @Override
                        public void process(String key, Context context, Iterable<Event> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                            long count = 0;
                            for (Event element : elements) {
                                count++;
                                System.out.println("Event processed: " + element.key + " at " + element.timestamp);
                            }
                            out.collect(new Tuple2<>(key, count));
                        }
                    }
            );

            counts.print(); // Print the results
            env.execute("Time Window Counter");

        }


    }

    public static class Event {
        public String key; // Or any identifier you want to count by
        public long timestamp;

        public Event() {} // Default constructor for Flink serialization
        public Event(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }
    }

}
