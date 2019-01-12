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
package io.prestosql.plugin.cassandra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class CassandraTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final CassandraTableHandle table;
    private final List<CassandraPartition> partitions;
    private final String clusteringPredicates;

    @JsonCreator
    public CassandraTableLayoutHandle(@JsonProperty("table") CassandraTableHandle table)
    {
        this(table, ImmutableList.of(), "");
    }

    public CassandraTableLayoutHandle(CassandraTableHandle table, List<CassandraPartition> partitions, String clusteringPredicates)
    {
        this.table = requireNonNull(table, "table is null");
        this.partitions = ImmutableList.copyOf(requireNonNull(partitions, "partition is null"));
        this.clusteringPredicates = requireNonNull(clusteringPredicates, "clusteringPredicates is null");
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

    @JsonIgnore
    public String getClusteringPredicates()
    {
        return clusteringPredicates;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
