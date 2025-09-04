package com.facebook.presto.hudi.partition;

import java.util.Collections;
import java.util.List;

/**
 * Extractor for a partition path from a single column.
 * <p>
 * This implementation extracts the partition value from the partition path as a single part
 * even if the relative partition path contains slashes, e.g., the `TimestampBasedKeyGenerator`
 * transforms the timestamp column into the partition path in the format of "yyyyMM/dd/HH".
 * The slash (`/`) is replaced with dash (`-`), e.g., `202210/01/20` -> `202210-01-20`.
 */
public class SinglePartPartitionValueExtractor implements PartitionValueExtractor {
    @Override
    public List<String> extractPartitionValuesInPath(String partitionPath) {
        return Collections.singletonList(partitionPath.replace('/', '-'));
    }
}
