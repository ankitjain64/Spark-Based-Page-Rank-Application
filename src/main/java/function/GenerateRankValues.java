package function;

import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Used by flat map on pairRdd to generate terms contributing to rank of each
 * page
 */
public class GenerateRankValues implements FlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Double>>, Tuple2<String, Double>> {
    public Iterator<Tuple2<String, Double>> call(Tuple2<String, Tuple2<Iterable<String>, Double>> vertexVsRankAndAdjacent) throws Exception {
        List<Tuple2<String, Double>> rv = new ArrayList<Tuple2<String, Double>>();
        Tuple2<Iterable<String>, Double> adjacentAndRank = vertexVsRankAndAdjacent._2();
        Iterable<String> adjacentNodes = adjacentAndRank._1();
        List<String> adjacentNodeList = new ArrayList<String>();
        Double currentNodeIdRank = adjacentAndRank._2();
        for (String adjacentNode : adjacentNodes) {
            adjacentNodeList.add(adjacentNode);
        }
        for (String adjacentNode : adjacentNodeList) {
            Double newRankTerm = 0.85 * (currentNodeIdRank / adjacentNodeList.size());
            rv.add(new Tuple2<String, Double>(adjacentNode, newRankTerm));
        }
        return rv.iterator();
    }
}
