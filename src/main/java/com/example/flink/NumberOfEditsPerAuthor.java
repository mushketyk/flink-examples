package com.example.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.util.Collector;

/**
 * Example of how to calculate a stream with a number of changes that every user performs every minute
 */
public class NumberOfEditsPerAuthor {
    public static void main(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

        edits
            .keyBy((KeySelector<WikipediaEditEvent, String>) WikipediaEditEvent::getUser)
            .timeWindow(Time.minutes(10))
            .apply(new WindowFunction<WikipediaEditEvent, Tuple2<String, Long>, String, TimeWindow>() {
                @Override
                public void apply(String userName, TimeWindow timeWindow, Iterable<WikipediaEditEvent> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                    long changesCount = 0;

                    for (WikipediaEditEvent ignored : iterable) {
                        changesCount++;
                    }

                    collector.collect(new Tuple2<>(userName, changesCount));
                }
            })
            .print();

        env.execute();
    }
}
