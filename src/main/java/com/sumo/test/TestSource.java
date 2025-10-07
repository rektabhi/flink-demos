package com.sumo.test;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;

public class TestSource extends RichParallelSourceFunction<Tuple2<String, String>> {

    // flag indicating whether source is still running
    private boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<String, String>> srcCtx) throws Exception {

        while (running) {
            srcCtx.collect(new Tuple2<>("A", "B"));
            // wait for 100 ms
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
