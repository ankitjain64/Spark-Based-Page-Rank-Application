package algo;

import function.*;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import partitioner.StringCustomPartitioner;
import scala.Tuple2;

import static utils.Constants.*;

/**
 * Implementation of page rank with custom paritioning and caching
 */
public class QThreeRunner_1 implements PageRank {

    public static final int NUM_PARTITIONS = 20;
    private static final String OUTPUT_PATH = HDFS_PREFIX + "/spark_output_3";

    public void run(JavaSparkContext javaSparkContext, String inputPath, Integer numIterations) {
        run(javaSparkContext, inputPath, numIterations, NUM_PARTITIONS);
    }

    public void run(JavaSparkContext javaSparkContext, String inputPath, Integer numIterations, Integer numPartitions) {
        StringCustomPartitioner customPartitioner = new StringCustomPartitioner(numPartitions);
        JavaRDD<String> inputRdd = javaSparkContext.textFile(inputPath, numPartitions)
                .cache().setName(INPUT);
        //vertex rank
        JavaPairRDD<String, Double> vertexVsRank = inputRdd.flatMap(new ExtractVertices()).setName(VERTICES_WITH_DUPLICATES)
                .distinct().setName(VERTICES_WITH_DUPLICATES)
                .mapToPair(new InitializeRank()).partitionBy(customPartitioner).setName
                        (VERTEX_VS_RANK_INITIAL);
        //From to To vertices
        JavaPairRDD<String, Iterable<String>> fromVsToVertices = inputRdd.mapToPair(new CreateEdgeTuple()).setName(EDGE_LIST_PAIR)
                .groupByKey(customPartitioner).partitionBy(customPartitioner).cache().setName
                        (FROM_VS_TO_VERTEX);

        for (int i = 0; i < numIterations; i++) {
            JavaPairRDD<String, Tuple2<Iterable<String>, Double>>
                    vertexVsRankAndAdjacent = fromVsToVertices.join(vertexVsRank).setName(JOIN_PREFIX + i);

            JavaRDD<Tuple2<String, Double>> intermediateRdd = vertexVsRankAndAdjacent.flatMap(new GenerateRankValues())
                    .setName(INTERMEDIATE_PREFIX + i);

            vertexVsRank = intermediateRdd.mapToPair(new TupleIdentityFunction()).setName(INTERMEDIATE_PREFIX + "pair" + i)
                    .reduceByKey(customPartitioner, new AdditionReduction())
                    .setName(INTERMEDIATE_PREFIX + "pair_constant_" + i)
                    .mapValues(new AddConstant()).setName(VERTEX_VS_RANK_PREFIX + i);
        }
        vertexVsRank.saveAsHadoopFile(OUTPUT_PATH, String.class, Double.class, TextOutputFormat.class);
    }
}
