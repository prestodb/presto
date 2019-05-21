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

import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.util.Objects.requireNonNull;

/**
 * HiveSplitPartitionInfo is a class for fields that are shared between all InternalHiveSplits
 * of the same partition. It allows the memory usage to only be counted once per partition
 */
public class HiveSplitPartitionInfo
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HiveSplitPartitionInfo.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();

    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final String partitionName;
    private final Map<Integer, HiveTypeName> columnCoercions;
    private final Optional<HiveSplit.BucketConversion> bucketConversion;

    // keep track of how many InternalHiveSplits reference this PartitionInfo.
    private final AtomicInteger references = new AtomicInteger(0);

    public HiveSplitPartitionInfo(
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            String partitionName,
            Map<Integer, HiveTypeName> columnCoercions,
            Optional<HiveSplit.BucketConversion> bucketConversion)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.columnCoercions = requireNonNull(columnCoercions, "columnCoersions is null");
        this.bucketConversion = requireNonNull(bucketConversion, "bucketConversion is null");
    }

    public Properties getSchema()
    {
        return schema;
    }

    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public Map<Integer, HiveTypeName> getColumnCoercions()
    {
        return columnCoercions;
    }

    public Optional<HiveSplit.BucketConversion> getBucketConversion()
    {
        return bucketConversion;
    }

    public int getEstimatedSizeInBytes()
    {
        int result = INSTANCE_SIZE;
        result += sizeOfObjectArray(partitionKeys.size());
        for (HivePartitionKey partitionKey : partitionKeys) {
            result += partitionKey.getEstimatedSizeInBytes();
        }

        result += partitionName.length() * Character.BYTES;
        result += sizeOfObjectArray(columnCoercions.size());
        for (HiveTypeName hiveTypeName : columnCoercions.values()) {
            result += INTEGER_INSTANCE_SIZE + hiveTypeName.getEstimatedSizeInBytes();
        }
        return result;
    }

    public int incrementAndGetReferences()
    {
        return references.incrementAndGet();
    }

    public int decrementAndGetReferences()
    {
        return references.decrementAndGet();
    }

}
