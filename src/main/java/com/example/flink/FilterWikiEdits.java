package com.example.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * Simple example of how to use streams in Apache Flink.
 * Reads a stream of wikipedia edits, filters them, and write to the standard output
 */
public class FilterWikiEdits {

    public static void main(String... args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

        edits.filter((FilterFunction<WikipediaEditEvent>) edit -> {
            return !edit.isBotEdit() && edit.getByteDiff() > 1000;
        })
        .print();

        env.execute();
    }
}
