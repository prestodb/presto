/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A partition handle that combines a bucket-level handle with partition column values,
 * used for partition-aware grouped execution where each lifespan represents a
 * (bucket, partition-values) pair instead of just a bucket.
 *
 * This class lives in presto-hive (rather than presto-spi) because it is only
 * created and consumed by the Hive connector (HiveNodePartitioningProvider and
 * HiveSplitSource). The scheduler in presto-main-base treats these as opaque
 * ConnectorPartitionHandle instances.
 *
 * The partition values map contains only the partition columns identified as usable
 * by the GroupedExecutionTagger, enabling partial partition key support.
 * For example, if a table is partitioned by (ds, hr) but only ds is usable,
 * partitionValues would be {"ds": "2024-01-01"}.
 */
public class CompoundPartitionHandle
        extends ConnectorPartitionHandle
{
    private final ConnectorPartitionHandle bucketHandle;
    private final Map<String, String> partitionValues;

    public CompoundPartitionHandle(ConnectorPartitionHandle bucketHandle, Map<String, String> partitionValues)
    {
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.partitionValues = ImmutableMap.copyOf(requireNonNull(partitionValues, "partitionValues is null"));
    }

    public ConnectorPartitionHandle getBucketHandle()
    {
        return bucketHandle;
    }

    public Map<String, String> getPartitionValues()
    {
        return partitionValues;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompoundPartitionHandle that = (CompoundPartitionHandle) o;
        return Objects.equals(bucketHandle, that.bucketHandle) &&
                Objects.equals(partitionValues, that.partitionValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketHandle, partitionValues);
    }

    @Override
    public String toString()
    {
        return "CompoundPartitionHandle{bucket=" + bucketHandle + ", partitionValues=" + partitionValues + "}";
    }
}
