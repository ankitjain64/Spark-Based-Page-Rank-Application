package partitioner;

import org.apache.spark.Partitioner;

/**
 * Custom Partitioner for Q2 and Q3
 */
public class StringCustomPartitioner extends Partitioner {
    private int numPartitions;

    public StringCustomPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int numPartitions() {
        return this.numPartitions;
    }

    public int getPartition(Object key) {
        String stringValue = String.valueOf(key);
        int hashCode = stringValue.hashCode() % this.numPartitions;
        //Even if the key is positive integer in string, it's hashCode of
        // string can give negative value
        if (hashCode < 0) {
            return hashCode + this.numPartitions;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        StringCustomPartitioner that = (StringCustomPartitioner) o;

        return this.numPartitions == that.numPartitions;
    }

    @Override
    public int hashCode() {
        return numPartitions;
    }
}
