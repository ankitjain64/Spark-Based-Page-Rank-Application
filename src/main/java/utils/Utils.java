package utils;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utils for stopping spark context and getting stacktrace as a string
 */
public class Utils {

    public static String getStackTraceAsString(Throwable th) {
        StringWriter sw = new StringWriter();
        th.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public static void closeSparkContext(JavaSparkContext javaSparkContext) throws InterruptedException {
        System.out.println("Attempting to close spark context and going for sleep");
        javaSparkContext.close();
        System.out.println("Closed Spark Context");

    }
}
