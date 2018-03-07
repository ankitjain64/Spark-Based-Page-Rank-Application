import algo.PageRank;
import algo.QOneRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import static java.lang.Integer.parseInt;
import static utils.Constants.APP_NAME;
import static utils.Constants.HDFS_PREFIX;
import static utils.Utils.closeSparkContext;
import static utils.Utils.getStackTraceAsString;

/**
 * Driver for PartC Question 1
 */
public class PartCQuestion_1 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME + "1");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        PageRank pageRankRunner = new QOneRunner();
        int exitStatus = 0;
        try {
            Integer numPartitions = 20;
            if (args.length == 3) {
                numPartitions = parseInt(args[2]);
            }
            pageRankRunner.run(javaSparkContext, HDFS_PREFIX + args[0],
                    parseInt(args[1]), numPartitions);
        } catch (Throwable th) {
            System.out.println("Application exited with following stack " +
                    "trace: " + getStackTraceAsString(th));
            exitStatus = -1;
        } finally {
            closeSparkContext(javaSparkContext);
        }
        System.exit(exitStatus);
    }
}
