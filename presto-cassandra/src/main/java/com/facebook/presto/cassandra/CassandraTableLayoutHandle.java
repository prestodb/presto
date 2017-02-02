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
package com.facebook.presto.cassandra;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class CassandraTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final CassandraTableHandle table;
    private final List<CassandraPartition> partitions;

    @JsonCreator
    public CassandraTableLayoutHandle(@JsonProperty("table") CassandraTableHandle table)
    {
        this(table, ImmutableList.of());
    }

    public CassandraTableLayoutHandle(CassandraTableHandle table, List<CassandraPartition> partitions)
    {
        this.table = requireNonNull(table, "table is null");
        this.partitions = requireNonNull(partitions, "partition is null");
    }

    @JsonProperty
    public CassandraTableHandle getTable()
    {
        return table;
    }

    @JsonIgnore
    public List<CassandraPartition> getPartitions()
    {
        return partitions;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
