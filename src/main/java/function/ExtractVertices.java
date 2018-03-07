package function;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

import static utils.Constants.INPUT_SEPERATOR;

/**
 * Used by flat map to extract all the vertices.
 */
public class ExtractVertices implements FlatMapFunction<String, String> {

    public Iterator<String> call(String s) throws Exception {
        String[] split = s.split(INPUT_SEPERATOR);
        return Arrays.asList(split).iterator();
    }
}
