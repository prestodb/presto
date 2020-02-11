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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CassandraSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String partitionId;
    private final List<HostAddress> addresses;
    private final String schema;
    private final String table;
    private final String splitCondition;

    @JsonCreator
    public CassandraSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("partitionId") String partitionId,
            @JsonProperty("splitCondition") String splitCondition,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(partitionId, "partitionName is null");
        requireNonNull(addresses, "addresses is null");

        this.connectorId = connectorId;
        this.schema = schema;
        this.table = table;
        this.partitionId = partitionId;
        this.addresses = ImmutableList.copyOf(addresses);
        this.splitCondition = splitCondition;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getSplitCondition()
    {
        return splitCondition;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return addresses;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("hosts", addresses)
                .put("schema", schema)
                .put("table", table)
                .put("partitionId", partitionId)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(table)
                .addValue(partitionId)
                .toString();
    }

    public String getWhereClause()
    {
        if (partitionId.equals(CassandraPartition.UNPARTITIONED_ID)) {
            if (splitCondition != null) {
                return " WHERE " + splitCondition;
            }
            else {
                return "";
            }
        }
        else {
            if (splitCondition != null) {
                return " WHERE " + partitionId + " AND " + splitCondition;
            }
            else {
                return " WHERE " + partitionId;
            }
        }
    }

    public CassandraTableHandle getCassandraTableHandle()
    {
        return new CassandraTableHandle(connectorId, schema, table);
    }
}
