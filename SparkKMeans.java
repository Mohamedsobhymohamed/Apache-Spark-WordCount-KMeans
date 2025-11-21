package com.geekcap.javaworld.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SparkKMeans {

    // Initial centroids provided in the Lab Sheet
    private static final double[][] INITIAL_CENTROIDS = {
        {5.0, 3.0, 1.0, 0.0},
        {5.0, 2.0, 4.0, 1.0},
        {6.0, 3.0, 5.0, 2.0}
    };

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SparkKMeans <input_file> <output_file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("SparkKMeans").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. Load Data
        JavaRDD<String> lines = sc.textFile(args[0]);

        // 2. Parse data (Added FILTER to skip empty lines)
        JavaRDD<List<Double>> points = lines
            .filter(line -> line != null && !line.trim().isEmpty()) // <--- THE FIX
            .map(line -> {
                String[] parts = line.split(",");
                List<Double> point = new ArrayList<>();
                for (String p : parts) {
                    // Extra check to ensure we don't parse whitespace
                    if (!p.trim().isEmpty()) {
                        point.add(Double.parseDouble(p.trim()));
                    }
                }
                return point;
            }).cache(); 

        // Initialize Centroids
        List<List<Double>> centroids = new ArrayList<>();
        for (double[] c : INITIAL_CENTROIDS) {
            List<Double> centroid = new ArrayList<>();
            for (double val : c) centroid.add(val);
            centroids.add(centroid);
        }

        // 3. Iteration Loop (Run 5 times)
        for (int i = 0; i < 5; i++) {
            final List<List<Double>> currentCentroids = centroids;
            
            Broadcast<List<List<Double>>> broadcastCentroids = sc.broadcast(currentCentroids);

            JavaPairRDD<Integer, Tuple2<List<Double>, Integer>> assigned = points.mapToPair(point -> {
                List<List<Double>> centers = broadcastCentroids.value();
                int bestIndex = 0;
                double closestDist = Double.MAX_VALUE;

                for (int j = 0; j < centers.size(); j++) {
                    double dist = euclideanDistance(point, centers.get(j));
                    if (dist < closestDist) {
                        closestDist = dist;
                        bestIndex = j;
                    }
                }
                return new Tuple2<>(bestIndex, new Tuple2<>(point, 1));
            });

            JavaPairRDD<Integer, Tuple2<List<Double>, Integer>> stats = assigned.reduceByKey((a, b) -> {
                List<Double> sum = new ArrayList<>();
                for (int k = 0; k < a._1().size(); k++) {
                    sum.add(a._1().get(k) + b._1().get(k));
                }
                return new Tuple2<>(sum, a._2() + b._2());
            });

            Map<Integer, Tuple2<List<Double>, Integer>> results = stats.collectAsMap();

            for (Map.Entry<Integer, Tuple2<List<Double>, Integer>> entry : results.entrySet()) {
                int index = entry.getKey();
                List<Double> sumVector = entry.getValue()._1();
                int count = entry.getValue()._2();
                
                List<Double> newCentroid = new ArrayList<>();
                for (Double val : sumVector) {
                    newCentroid.add(val / count);
                }
                centroids.set(index, newCentroid);
            }
            
            System.out.println("Iteration " + (i+1) + " finished.");
        }

        // 4. Save Final Centroids
        List<String> outputLines = new ArrayList<>();
        outputLines.add("Final Centroids:");
        for (List<Double> c : centroids) {
            outputLines.add(c.toString());
        }
        
        sc.parallelize(outputLines).saveAsTextFile(args[1]);
        sc.close();
    }

    private static double euclideanDistance(List<Double> p1, List<Double> p2) {
        double sum = 0.0;
        for (int i = 0; i < p1.size(); i++) {
            sum += Math.pow(p1.get(i) - p2.get(i), 2);
        }
        return Math.sqrt(sum);
    }
}
