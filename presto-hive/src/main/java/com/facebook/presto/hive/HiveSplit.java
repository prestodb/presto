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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveSplit
        implements ConnectorSplit
{
    private final String clientId;
    private final String path;
    private final long start;
    private final long length;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final List<HostAddress> addresses;
    private final String database;
    private final String table;
    private final String partitionName;
    private final ConnectorSession session;
    private final TupleDomain<HiveColumnHandle> tupleDomain;

    @JsonCreator
    public HiveSplit(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("database") String database,
            @JsonProperty("table") String table,
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("schema") Properties schema,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("session") ConnectorSession session,
            @JsonProperty("tupleDomain") TupleDomain<HiveColumnHandle> tupleDomain)
    {
        checkNotNull(clientId, "clientId is null");
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkNotNull(database, "database is null");
        checkNotNull(table, "table is null");
        checkNotNull(partitionName, "partitionName is null");
        checkNotNull(path, "path is null");
        checkNotNull(schema, "schema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(addresses, "addresses is null");
        checkNotNull(tupleDomain, "tupleDomain is null");

        this.clientId = clientId;
        this.database = database;
        this.table = table;
        this.partitionName = partitionName;
        this.path = path;
        this.start = start;
        this.length = length;
        this.schema = schema;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.addresses = ImmutableList.copyOf(addresses);
        this.session = session;
        this.tupleDomain = tupleDomain;
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    public String getDatabase()
    {
        return database;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public Properties getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public ConnectorSession getSession()
    {
        return session;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getTupleDomain()
    {
        return tupleDomain;
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
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .put("hosts", addresses)
                .put("database", database)
                .put("table", table)
                .put("partitionName", partitionName)
                .put("tupleDomain", tupleDomain)
                .build();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(tupleDomain)
                .toString();
    }
}
