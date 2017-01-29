package com.example.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * Implementing word count example using Java 8 lambdas
 */
public class Java8WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lines = env.fromElements(
            "Apache Flink is a community-driven open source framework for distributed big data analytics,",
            "like Hadoop and Spark. The core of Apache Flink is a distributed streaming dataflow engine written",
            " in Java and Scala.[1][2] It aims to bridge the gap between MapReduce-like systems and shared-nothing",
            "parallel database systems. Therefore, Flink executes arbitrary dataflow programs in a data-parallel and",
            "pipelined manner.[3] Flink's pipelined runtime system enables the execution of bulk/batch and stream",
            "processing programs.[4][5] Furthermore, Flink's runtime supports the execution of iterative algorithms natively.[6]"
        );

        lines.flatMap((line, out) -> {
            String[] words = line.split("\\W+");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        })
        .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)))
        .groupBy(0)
        .sum(1)
        .print();
    }
}
