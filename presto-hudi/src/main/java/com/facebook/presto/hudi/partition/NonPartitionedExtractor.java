package com.facebook.presto.hudi.partition;

import java.util.ArrayList;
import java.util.List;

/**
 * Extractor for Non-partitioned hive tables.
 */
public class NonPartitionedExtractor implements PartitionValueExtractor {

    @Override
    public List<String> extractPartitionValuesInPath(String partitionPath) {
        return new ArrayList<>();
    }
}
