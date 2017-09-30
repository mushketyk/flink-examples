package com.example.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Example of how to calculate a number of changes that performed in Wikipedia every minute
 */
public class NumberOfWikiEditsPerWindow {

    public static void main(String... args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

        edits
            .timeWindowAll(Time.minutes(1))
            .apply(new AllWindowFunction<WikipediaEditEvent, Tuple3<Date, Long, Long>, TimeWindow>() {
                @Override
                public void apply(TimeWindow timeWindow, Iterable<WikipediaEditEvent> iterable, Collector<Tuple3<Date, Long, Long>> collector) throws Exception {
                    long count = 0;
                    long bytesChanged = 0;

                    for (WikipediaEditEvent event : iterable) {
                        count++;
                        bytesChanged += event.getByteDiff();
                    }

                    collector.collect(new Tuple3<>(new Date(timeWindow.getEnd()), count, bytesChanged));
                }
            })
            .print();


        env.execute();
    }
}
