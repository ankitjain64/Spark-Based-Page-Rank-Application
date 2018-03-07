package function;

import org.apache.spark.api.java.function.Function;

/**
 * Used by map values to add the constant term during rank
 */
public class AddConstant implements Function<Double, Double> {

    private static final double CONSTANT_PAGE_RANK_CONTRIBUTION = 0.15;

    public Double call(Double aDouble) throws Exception {
        return CONSTANT_PAGE_RANK_CONTRIBUTION + aDouble;
    }
}
