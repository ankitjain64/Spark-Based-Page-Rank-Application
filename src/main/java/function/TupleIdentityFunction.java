package function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Used by mapToPair function as identity relation for converting values to
 * key value pair rdd.
 */
public class TupleIdentityFunction implements PairFunction<Tuple2<String, Double>, String, Double> {
    public Tuple2<String, Double> call(Tuple2<String, Double> value) throws Exception {
        return value;
    }
}
