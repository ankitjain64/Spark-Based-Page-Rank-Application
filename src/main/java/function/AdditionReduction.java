package function;

import org.apache.spark.api.java.function.Function2;

/**
 * Used for adding all the terms of page rank together
 */
public class AdditionReduction implements Function2<Double, Double, Double> {
    public Double call(Double contributionOne, Double contributionTwo) throws Exception {
        return contributionOne + contributionTwo;
    }
}
