package partitioner;

import org.apache.spark.Partitioner;

import static java.lang.Integer.*;

/**
 * Partitioner which parses the string key to integer and returns modulo
 * based on number of partitions
 */
public class IntCustomPartitioner extends Partitioner {
    private int numPartitions;

    public IntCustomPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int numPartitions() {
        return this.numPartitions;
    }

    public int getPartition(Object key) {
        int intKey = parseInt((String) key);
        /*
          Since keys are node id's and they are already positive we need not
          take care of negatives here
         */
        return intKey % this.numPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        IntCustomPartitioner that = (IntCustomPartitioner) o;

        return this.numPartitions == that.numPartitions;
    }

    @Override
    public int hashCode() {
        return numPartitions;
    }
}
