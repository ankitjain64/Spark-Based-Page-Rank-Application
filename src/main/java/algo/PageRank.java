package algo;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Interface for exposing spark based page rank algorithm
 */
public interface PageRank extends Serializable {

    void run(JavaSparkContext javaSparkContext, String inputPath, Integer numIterations);

    void run(JavaSparkContext javaSparkContext, String inputPath, Integer
            numIterations, Integer numPartitions);
}
