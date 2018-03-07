import algo.PageRank;
import algo.QThreeRunner_1;
import algo.QTwoRunner_1;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import static java.lang.Integer.parseInt;
import static utils.Constants.APP_NAME;
import static utils.Constants.HDFS_PREFIX;
import static utils.Utils.closeSparkContext;
import static utils.Utils.getStackTraceAsString;

/**
 * Driver for PartC Question 3 with Custom Partitioner as String Custom
 * Partitioner
 */
public class PartCQuestion_3_1 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME + "3");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        PageRank pageRankRunner = new QThreeRunner_1();
        int exitStatus = 0;
        try {
            Integer partitions = extractPartitions(args);
            pageRankRunner.run(javaSparkContext, HDFS_PREFIX + args[0],
                    parseInt(args[1]), partitions);
        } catch (Throwable th) {
            System.out.println("Application exited with following stack " +
                    "trace: " + getStackTraceAsString(th));
            exitStatus = -1;
        } finally {
            closeSparkContext(javaSparkContext);
        }
        System.exit(exitStatus);

    }

    private static Integer extractPartitions(String[] args) {
        Integer partitions;
        if (args.length < 3) {
            partitions = QThreeRunner_1.NUM_PARTITIONS;
        } else {
            partitions = Integer.parseInt(args[2]);
        }
        return partitions;
    }
}
