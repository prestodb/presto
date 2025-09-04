package com.facebook.presto.hudi.partition;

import java.io.Serializable;
import java.util.List;

/**
 * HDFS Path contain hive partition values for the keys it is partitioned on. This mapping is not straight forward and
 * requires a pluggable implementation to extract the partition value from HDFS path.
 * <p>
 * e.g. Hive table partitioned by datestr=yyyy-mm-dd and hdfs path /app/hoodie/dataset1/YYYY=[yyyy]/MM=[mm]/DD=[dd]
 */
public interface PartitionValueExtractor extends Serializable
{

    List<String> extractPartitionValuesInPath(String partitionPath);
}
