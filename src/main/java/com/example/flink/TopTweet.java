package com.example.flink;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

public class TopTweet {

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "...");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "...");
        props.setProperty(TwitterSource.TOKEN, "...");
        props.setProperty(TwitterSource.TOKEN_SECRET, "...");

        env.addSource(new TwitterSource(props))
            .flatMap(new ExtractHashTags())
            .keyBy(0)
            .timeWindow(Time.seconds(30))
            .sum(1)
            .filter(new FilterHashTags())
            .timeWindowAll(Time.seconds(30))
            .apply(new GetTopHashTag())
            .print();

        env.execute();
    }

    private static class TweetsCount implements Serializable {
        private static final long serialVersionUID = 1L;
        private Date windowStart;
        private Date windowEnd;
        private String hashTag;
        private int count;

        public TweetsCount(long windowStart, long windowEnd, String hashTag, int count) {
            this.windowStart = new Date(windowStart);
            this.windowEnd = new Date(windowEnd);
            this.hashTag = hashTag;
            this.count = count;
        }

        @Override
        public String toString() {
            return "TweetsCount{" +
                    "windowStart=" + windowStart +
                    ", windowEnd=" + windowEnd +
                    ", hashTag='" + hashTag + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    private static class ExtractHashTags implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void flatMap(String tweetJsonStr, Collector<Tuple2<String, Integer>> collector) throws Exception {
            JsonNode tweetJson = mapper.readTree(tweetJsonStr);
            JsonNode entities = tweetJson.get("entities");
            if (entities == null) return;

            JsonNode hashtags = entities.get("hashtags");
            if (hashtags == null) return;

            for (Iterator<JsonNode> iter = hashtags.getElements(); iter.hasNext();) {
                JsonNode node = iter.next();
                String hashtag = node.get("text").getTextValue();

                if (hashtag.matches("\\w+")) {
                    collector.collect(new Tuple2<>(hashtag, 1));
                }
            }
        }
    }

    private static class FilterHashTags implements FilterFunction<Tuple2<String, Integer>> {
        @Override
        public boolean filter(Tuple2<String, Integer> hashTag) throws Exception {
            return hashTag.f1 != 1;
        }
    }

private static class GetTopHashTag implements AllWindowFunction<Tuple2<String,Integer>, TweetsCount, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> hashTags, Collector<TweetsCount> out) throws Exception {
        Tuple2<String, Integer> topHashTag = new Tuple2<>("", 0);
        for (Tuple2<String, Integer> hashTag : hashTags) {
            if (hashTag.f1 > topHashTag.f1) {
                topHashTag = hashTag;
            }
        }

        out.collect(new TweetsCount(window.getStart(), window.getEnd(), topHashTag.f0, topHashTag.f1));
    }
}
}
