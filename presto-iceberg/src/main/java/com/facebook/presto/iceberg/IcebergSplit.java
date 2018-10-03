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

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

public class IcebergSplit
        implements ConnectorSplit
{
    private final String database;
    private final String table;
    private final String path;
    private final long start;
    private final long length;
    private final Map<String, Integer> nameToId;
    private final List<HostAddress> addresses;
    private final TupleDomain<HiveColumnHandle> effectivePredicate;
    private final List<HivePartitionKey> partitionKeys;
    private final boolean forceLocalScheduling;

    @JsonCreator
    public IcebergSplit(
            @JsonProperty("database") String database,
            @JsonProperty("table") String table,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("nameToId") Map<String, Integer> nameToId,
            @JsonProperty("effectivePredicate") TupleDomain<HiveColumnHandle> effectivePredicate,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("forceLocalScheduling") boolean forceLocalScheduling)
    {
        this.database = database;
        this.table = table;
        this.path = path;
        this.start = start;
        this.length = length;
        this.nameToId = nameToId;
        this.addresses = addresses;
        this.effectivePredicate = effectivePredicate;
        this.partitionKeys = partitionKeys;
        this.forceLocalScheduling = forceLocalScheduling;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("database", database)
                .put("table", table)
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .put("hosts", addresses)
                .put("forceLocalScheduling", forceLocalScheduling)
                .build();
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
    public Map<String, Integer> getNameToId()
    {
        return nameToId;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    public boolean isForceLocalScheduling()
    {
        return forceLocalScheduling;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(effectivePredicate)
                .toString();
    }
}
