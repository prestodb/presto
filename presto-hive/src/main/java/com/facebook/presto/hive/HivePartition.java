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

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HivePartition
{
    public static final String UNPARTITIONED_ID = "<UNPARTITIONED>";

    private final SchemaTableName tableName;
    private final String partitionId;
    private final Map<ColumnHandle, NullableValue> keys;
    private final Optional<Partition> metastorePartition;

    public HivePartition(SchemaTableName tableName)
    {
        this(tableName, UNPARTITIONED_ID, ImmutableMap.of(), Optional.empty());
    }

    public HivePartition(
            SchemaTableName tableName,
            String partitionId,
            Map<ColumnHandle, NullableValue> keys,
            Optional<Partition> metastorePartition)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.keys = ImmutableMap.copyOf(requireNonNull(keys, "keys is null"));
        this.metastorePartition = requireNonNull(metastorePartition, "metastorePartition is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public String getPartitionId()
    {
        return partitionId;
    }

    public Map<ColumnHandle, NullableValue> getKeys()
    {
        return keys;
    }

    public boolean hasMetastorePartition()
    {
        return metastorePartition.isPresent();
    }

    public Optional<Partition> getMetastorePartition()
    {
        return metastorePartition;
    }

    public HivePartition withMetastorePartition(Partition metastorePartition)
    {
        checkState(!this.metastorePartition.isPresent() || metastorePartition.equals(this.metastorePartition.get()), "Metastore partition is already present and != to new one");
        return new HivePartition(tableName, partitionId, keys, Optional.of(metastorePartition));
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
