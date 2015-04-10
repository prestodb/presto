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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

// Adaptation layer for connectors that implement the old getPartitions and getPartitionSplits API
// TODO: remove once all connectors migrate to getTableLayouts/getSplits API
public final class LegacyTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ConnectorTableHandle table;
    private final List<ConnectorPartition> partitions;

    @JsonCreator
    public LegacyTableLayoutHandle(@JsonProperty("table") ConnectorTableHandle table)
    {
        requireNonNull(table, "table is null");

        this.table = table;
        this.partitions = null;
    }

    public LegacyTableLayoutHandle(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        requireNonNull(table, "table is null");
        requireNonNull(partitions, "partitions is null");

        this.table = table;
        this.partitions = partitions;
    }

    @JsonProperty
    public ConnectorTableHandle getTable()
    {
        return table;
    }

    public List<ConnectorPartition> getPartitions()
    {
        checkState(partitions != null, "Partitions dropped by serialization");
        return partitions;
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
        LegacyTableLayoutHandle that = (LegacyTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, partitions);
    }
}
