package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

// Get distribution of ratings from MovieLens 100K Dataset:
// http://grouplens.org/datasets/movielens/
public class RatingsDistribution {

    public static void main(String[] args) throws Exception {

        // parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        // path to ratings.csv file
        String ratingsCsvPath = params.getRequired("input");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> file = env.readTextFile(ratingsCsvPath);
        file.flatMap(new ExtractRating())
            .groupBy(0)
            // .reduceGroup(new SumRatingCount())
            .sum(1)
            .print();
    }

private static class ExtractRating implements FlatMapFunction<String, Tuple2<IntValue, Integer>> {
    IntValue ratingValue = new IntValue();

    // Reuse rating value and result tuple
    Tuple2<IntValue, Integer> result = new Tuple2<>(ratingValue, 1);

    @Override
    public void flatMap(String s, Collector<Tuple2<IntValue, Integer>> collector) throws Exception {
        // Every line contains tab separated values
        // user id | item id | rating | timestamp
        String[] split = s.split(",");
        String ratingStr = split[2];

        if (!ratingStr.equals("rating")) {
            int rating = (int) Double.parseDouble(split[2]);
            ratingValue.setValue(rating);

            collector.collect(result);
        }
    }
}

private static class SumRatingCount implements GroupReduceFunction<Tuple2<IntValue, Integer>, Tuple2<IntValue, Integer>> {
    @Override
    public void reduce(Iterable<Tuple2<IntValue, Integer>> iterable, Collector<Tuple2<IntValue, Integer>> collector) throws Exception {
        IntValue rating = null;
        int ratingsCount = 0;
        for (Tuple2<IntValue, Integer> tuple : iterable) {
            rating = tuple.f0;
            ratingsCount += tuple.f1;
        }

        collector.collect(new Tuple2<>(rating, ratingsCount));
    }
}
}
