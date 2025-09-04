package com.facebook.presto.hudi.partition;

import java.util.Collections;
import java.util.List;

/**
 * Extractor for Hive Style Partitioned tables, when the partition folders are key value pairs.
 *
 * <p>This implementation extracts the partition value of yyyy-mm-dd from the path of type datestr=yyyy-mm-dd.
 */
public class HiveStylePartitionValueExtractor implements PartitionValueExtractor {
    private static final long serialVersionUID = 1L;

    @Override
    public List<String> extractPartitionValuesInPath(String partitionPath) {
        // partition path is expected to be in this format partition_key=partition_value.
        String[] splits = partitionPath.split("=");
        if (splits.length != 2) {
            throw new IllegalArgumentException(
                    "Partition path " + partitionPath + " is not in the form partition_key=partition_value.");
        }
        return Collections.singletonList(splits[1]);
    }
}