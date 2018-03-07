package function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Used by map to pair to assign an initial rank of 1 to all the vertices.
 * Basically a pair rdd with key as nodeId and value as it's rank
 */
public class InitializeRank implements PairFunction<String, String, Double> {
    public Tuple2<String, Double> call(String s) throws Exception {
        return new Tuple2<String, Double>(s, 1d);
    }
}
