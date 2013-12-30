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

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.PartitionedSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraSplit
        implements PartitionedSplit
{
    private final String connectorId;
    private final String partitionId;
    private final List<HostAddress> addresses;
    private final String schema;
    private final String table;
    private final boolean lastSplit;
    private final String splitCondition;

    @JsonCreator
    public CassandraSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("partitionId") String partitionId,
            @JsonProperty("splitCondition") String splitCondition,
            @JsonProperty("lastSplit") boolean lastSplit,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(schema, "schema is null");
        checkNotNull(table, "table is null");
        checkNotNull(partitionId, "partitionName is null");
        checkNotNull(addresses, "addresses is null");

        this.connectorId = connectorId;
        this.schema = schema;
        this.table = table;
        this.partitionId = partitionId;
        this.addresses = ImmutableList.copyOf(addresses);
        this.lastSplit = lastSplit;
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
    @Override
    public String getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    @Override
    public boolean isLastSplit()
    {
        return lastSplit;
    }

    @JsonProperty
    @Override
    public List<PartitionKey> getPartitionKeys()
    {
        return ImmutableList.of();
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
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
        return Objects.toStringHelper(this)
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
