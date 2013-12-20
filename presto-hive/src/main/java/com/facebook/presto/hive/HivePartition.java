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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.google.common.base.Preconditions.checkNotNull;

public class HivePartition
        implements Partition
{
    public static final String UNPARTITIONED_ID = "<UNPARTITIONED>";

    private final SchemaTableName tableName;
    private final String partitionId;
    private final Map<ColumnHandle, Comparable<?>> keys;
    private final Optional<HiveBucket> bucket;

    public HivePartition(SchemaTableName tableName)
    {
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionId = UNPARTITIONED_ID;
        this.keys = ImmutableMap.of();
        this.bucket = Optional.absent();
    }

    public HivePartition(SchemaTableName tableName, String partitionId, Map<ColumnHandle, Comparable<?>> keys, Optional<HiveBucket> bucket)
    {
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionId = checkNotNull(partitionId, "partitionId is null");
        this.keys = ImmutableMap.copyOf(checkNotNull(keys, "keys is null"));
        this.bucket = checkNotNull(bucket, "bucket number is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @Override
    public String getPartitionId()
    {
        return partitionId;
    }

    @Override
    public TupleDomain getTupleDomain()
    {
        return TupleDomain.withFixedValues(keys);
    }

    public Map<ColumnHandle, Comparable<?>> getKeys()
    {
        return keys;
    }

    public Optional<HiveBucket> getBucket()
    {
        return bucket;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HivePartition other = (HivePartition) obj;
        return Objects.equals(this.partitionId, other.partitionId);
    }

    @Override
    public String toString()
    {
        return tableName + ":" + partitionId;
    }
}
