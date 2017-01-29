package com.example.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Get top 10 movies from a movie ratings dataset.
 */
public class Top10Movies {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Double>> sorted = env.readCsvFile("/Users/ivanmushketyk/Flink/ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .includeFields(false, true, true, false)
                .types(Integer.class, Double.class)
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer,Double>, Tuple2<Integer, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Double>> values, Collector<Tuple2<Integer, Double>> out) throws Exception {
                        Integer movieId = null;
                        double total = 0;
                        int count = 0;
                        for (Tuple2<Integer, Double> value : values) {
                            movieId = value.f0;
                            total += value.f1;
                            count++;
                        }

                        if (count > 50) {
                            out.collect(new Tuple2<>(movieId, total / count));
                        }

                    }
                })
                .partitionCustom(new Partitioner<Double>() {
                    @Override
                    public int partition(Double key, int numPartitions) {
                        return key.intValue() % numPartitions;
                    }
                }, 1)
                .setParallelism(5)
                .sortPartition(1, Order.DESCENDING)
                .mapPartition(new MapPartitionFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Integer, Double>> values, Collector<Tuple2<Integer, Double>> out) throws Exception {
                        Iterator<Tuple2<Integer, Double>> iter = values.iterator();
                        for (int i = 0; i < 10 && iter.hasNext(); i++) {
                            out.collect(iter.next());
                        }
                    }
                })
                .sortPartition(1, Order.DESCENDING).setParallelism(1)
                .mapPartition(new MapPartitionFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Integer, Double>> values, Collector<Tuple2<Integer, Double>> out) throws Exception {
                        Iterator<Tuple2<Integer, Double>> iter = values.iterator();
                        for (int i = 0; i < 10 && iter.hasNext(); i++) {
                            out.collect(iter.next());
                        }
                    }
                });

        DataSet<Tuple2<Integer, String>> movies = env.readCsvFile("/Users/ivanmushketyk/Flink/ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .includeFields(true, true, false)
                .types(Integer.class, String.class);

        movies.first(10).print();

        DataSet<Tuple3<Integer, String, Double>> result = movies.join(sorted)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,Double>, Tuple3<Integer, String, Double>>() {
                    @Override
                    public Tuple3<Integer, String, Double> join(Tuple2<Integer, String> first, Tuple2<Integer, Double> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });

        result.print();
    }
}
