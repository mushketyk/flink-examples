package com.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

import java.util.Arrays;
import java.util.List;

public class PregelShortestPath {
    public static void main(String... args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Vertex<Integer, String>> vertices = Arrays.asList(
                new Vertex<>(1, "1"),
                new Vertex<>(2, "2"),
                new Vertex<>(3, "3"),
                new Vertex<>(4, "4"),
                new Vertex<>(5, "5")
        );

        List<Edge<Integer, Double>> edges = Arrays.asList(
                new Edge<>(1, 2, 2.0),
                new Edge<>(1, 3, 7.0),
                new Edge<>(2, 3, 2.0),
                new Edge<>(3, 2, 5.0),
                new Edge<>(2, 4, 4.0),
                new Edge<>(3, 4, 6.0),
                new Edge<>(3, 5, 3.0),
                new Edge<>(4, 5, 4.0),
                new Edge<>(5, 4, 1.0),
                new Edge<>(5, 1, 8.0)
        );

        Graph<Integer, String, Double> graph = Graph.fromCollection(vertices, edges, env);

        graph.run(new ShortestPath<>(1, 10)).print();
    }
}

class ShortestPath<K, VV> implements GraphAlgorithm<K, VV, Double, DataSet<Vertex<K, Double>>> {

    private final K sourceVertex;
    private final int maxIterations;

    public ShortestPath(K sourceVertex, int maxIterations) {
        this.sourceVertex = sourceVertex;
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, Double>> run(Graph<K, VV, Double> graph) throws Exception {
        Graph<K, Double, Double> resultGraph = graph.mapVertices(new VertexDoubleMapFunction<>(sourceVertex))
        .runVertexCentricIteration(new ShortestPathComputeFunction(sourceVertex),
                                   new ShortestPathCombiner(),
                                   maxIterations);
        return resultGraph.getVertices();
    }

    private static class VertexDoubleMapFunction<K, VV> implements MapFunction<Vertex<K,VV>, Double> {

        private final K sourceVertex;

        public VertexDoubleMapFunction(K sourceVertex) {
            this.sourceVertex = sourceVertex;
        }

        @Override
        public Double map(Vertex<K, VV> vertex) throws Exception {
            if (vertex.getId().equals(sourceVertex)) {
                return 0d;
            }
            return Double.MAX_VALUE;
        }
    }
}

class ShortestPathComputeFunction<K> extends ComputeFunction<K, Double, Double, NewMinDistance> {

    private final K sourceVertex;

    public ShortestPathComputeFunction(K sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    @Override
    public void compute(Vertex<K, Double> vertex, MessageIterator<NewMinDistance> messageIterator) throws Exception {
        // Send initial group of messages from the source vertex
        if (vertex.getId().equals(sourceVertex) && getSuperstepNumber() == 1) {
            sendNewDistanceToAll(0);
        }

        // Calculate new min distance from source node
        double minDistance = minDistance(messageIterator);

        // Send new min distance to neighbour vertices if new min distance is less
        if (minDistance < vertex.getValue()) {
            setNewVertexValue(minDistance);
            sendNewDistanceToAll(minDistance);
        }
    }

    private double minDistance(MessageIterator<NewMinDistance> messageIterator) {
        double minDistance = Double.MAX_VALUE;
        for (NewMinDistance message : messageIterator) {
            minDistance = Math.min(message.getDistance(), minDistance);
        }
        return minDistance;
    }

    private void sendNewDistanceToAll(double newDistance) {
        for (Edge<K, Double> edge : getEdges()) {
            sendMessageTo(edge.getTarget(), new NewMinDistance(edge.getValue() + newDistance));
        }
    }
}

/**
 * Message that contains new value of minimal distances for a particular path.
 */
class NewMinDistance {
    private final double distance;

    public NewMinDistance(double distance) {
        this.distance = distance;
    }

    public double getDistance() {
        return distance;
    }
}

/**
 * Combine multiple outgoing messages directed to the same vertex.
 * @param <K>
 */
class ShortestPathCombiner<K> extends MessageCombiner<K, NewMinDistance> {
    @Override
    public void combineMessages(MessageIterator<NewMinDistance> messageIterator) throws Exception {
        double minDistance = Double.MAX_VALUE;
        for (NewMinDistance message : messageIterator) {
            minDistance = Math.min(message.getDistance(), minDistance);
        }

        sendCombinedMessage(new NewMinDistance(minDistance));
    }
}

