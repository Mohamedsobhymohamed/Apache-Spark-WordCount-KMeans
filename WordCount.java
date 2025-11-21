package com.geekcap.javaworld.sparkexample;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

public class WordCount {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(1);
        }

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read input file
        JavaRDD<String> lines = sc.textFile(args[0]);

        // Split lines into words - Using explicit class to avoid compiler errors
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split("\\s+")).iterator();
            }
        });

        // Map words to (word, 1)
        JavaPairRDD<String, Integer> wordOnes = words.mapToPair(
            s -> new Tuple2<>(s, 1)
        );

        // Reduce by key (sum counts)
        JavaPairRDD<String, Integer> counts = wordOnes.reduceByKey(
            (a, b) -> a + b
        );

        // Save results
        counts.saveAsTextFile(args[1]);

        sc.close();
    }
}
