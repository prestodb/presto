package com.facebook.presto.hudi.partition;

import org.apache.hudi.common.util.ValidationUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Partition Key extractor treating each value delimited by slash as separate key.
 */
public class MultiPartKeysValueExtractor implements PartitionValueExtractor {

    @Override
    public List<String> extractPartitionValuesInPath(String partitionPath) {
        // If the partitionPath is empty string( which means none-partition table), the partition values
        // should be empty list.
        if (partitionPath.isEmpty()) {
            return Collections.emptyList();
        }
        String[] splits = partitionPath.split("/");
        return Arrays.stream(splits).map(s -> {
            if (s.contains("=")) {
                String[] moreSplit = s.split("=");
                ValidationUtils.checkArgument(moreSplit.length == 2, "Partition Field (" + s + ") not in expected format");
                return moreSplit[1];
            }
            return s;
        }).collect(Collectors.toList());
    }
}