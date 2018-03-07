package function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import static utils.Constants.INPUT_SEPERATOR;

/**
 * Used by map to Pair to create a pair rdd where key is from vertex and
 * value is to vertex.
 */
public class CreateEdgeTuple implements PairFunction<String, String, String> {
    public Tuple2<String, String> call(String s) throws Exception {
        String[] split = s.split(INPUT_SEPERATOR);
        return new Tuple2<String, String>(split[0], split[1]);
    }
}
