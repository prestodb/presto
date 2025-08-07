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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class IcebergPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final List<String> partitioning;
    private final List<IcebergColumnHandle> partitioningColumns;

    @JsonCreator
    public IcebergPartitioningHandle(
            @JsonProperty("partitioning") List<String> partitioning,
            @JsonProperty("partitioningColumns") List<IcebergColumnHandle> partitioningColumns)
    {
        this.partitioning = ImmutableList.copyOf(requireNonNull(partitioning, "partitioning is null"));
        this.partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
    }

    @JsonProperty
    public List<String> getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioning", partitioning)
                .toString();
    }
}
