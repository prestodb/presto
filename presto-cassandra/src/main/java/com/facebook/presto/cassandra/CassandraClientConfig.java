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

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CassandraClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private Duration schemaCacheTtl = new Duration(1, TimeUnit.HOURS);
    private Duration schemaRefreshInterval = new Duration(2, TimeUnit.MINUTES);
    private int maxSchemaRefreshThreads = 10;
    private int limitForPartitionKeySelect = 200;
    private int fetchSizeForPartitionKeySelect = 20_000;
    private int unpartitionedSplits = 1_000;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private int fetchSize = 5_000;
    private List<String> contactPoints = ImmutableList.of();
    private int nativeProtocolPort = 9042;
    private int partitionSizeForBatchSelect = 100;

    @Min(0)
    public int getLimitForPartitionKeySelect()
    {
        return limitForPartitionKeySelect;
    }

    @Config("cassandra.limit-for-partition-key-select")
    public CassandraClientConfig setLimitForPartitionKeySelect(int limitForPartitionKeySelect)
    {
        this.limitForPartitionKeySelect = limitForPartitionKeySelect;
        return this;
    }

    @Min(1)
    public int getUnpartitionedSplits()
    {
        return unpartitionedSplits;
    }

    @Config("cassandra.unpartitioned-splits")
    public CassandraClientConfig setUnpartitionedSplits(int unpartitionedSplits)
    {
        this.unpartitionedSplits = unpartitionedSplits;
        return this;
    }

    @Min(1)
    public int getMaxSchemaRefreshThreads()
    {
        return maxSchemaRefreshThreads;
    }

    @Config("cassandra.max-schema-refresh-threads")
    public CassandraClientConfig setMaxSchemaRefreshThreads(int maxSchemaRefreshThreads)
    {
        this.maxSchemaRefreshThreads = maxSchemaRefreshThreads;
        return this;
    }

    @NotNull
    public Duration getSchemaCacheTtl()
    {
        return schemaCacheTtl;
    }

    @Config("cassandra.schema-cache-ttl")
    public CassandraClientConfig setSchemaCacheTtl(Duration schemaCacheTtl)
    {
        this.schemaCacheTtl = schemaCacheTtl;
        return this;
    }

    @NotNull
    public Duration getSchemaRefreshInterval()
    {
        return schemaRefreshInterval;
    }

    @Config("cassandra.schema-refresh-interval")
    public CassandraClientConfig setSchemaRefreshInterval(Duration schemaRefreshInterval)
    {
        this.schemaRefreshInterval = schemaRefreshInterval;
        return this;
    }

    @NotNull
    @Size(min = 1)
    public List<String> getContactPoints()
    {
        return contactPoints;
    }

    @Config("cassandra.contact-points")
    public CassandraClientConfig setContactPoints(String commaSeparatedList)
    {
        this.contactPoints = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    public CassandraClientConfig setContactPoints(String... contactPoints)
    {
        this.contactPoints = Arrays.asList(contactPoints);
        return this;
    }

    @Min(1)
    public int getNativeProtocolPort()
    {
        return nativeProtocolPort;
    }

    @Config(("cassandra.native-protocol-port"))
    public CassandraClientConfig setNativeProtocolPort(int nativeProtocolPort)
    {
        this.nativeProtocolPort = nativeProtocolPort;
        return this;
    }

    @NotNull
    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    @Config("cassandra.consistency-level")
    public CassandraClientConfig setConsistencyLevel(ConsistencyLevel level)
    {
        this.consistencyLevel = level;
        return this;
    }

    @Min(1)
    public int getFetchSize()
    {
        return fetchSize;
    }

    @Config("cassandra.fetch-size")
    public CassandraClientConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    @Min(1)
    public int getFetchSizeForPartitionKeySelect()
    {
        return fetchSizeForPartitionKeySelect;
    }

    @Config("cassandra.fetch-size-for-partition-key-select")
    public CassandraClientConfig setFetchSizeForPartitionKeySelect(int fetchSizeForPartitionKeySelect)
    {
        this.fetchSizeForPartitionKeySelect = fetchSizeForPartitionKeySelect;
        return this;
    }

    @Min(1)
    public int getPartitionSizeForBatchSelect()
    {
        return partitionSizeForBatchSelect;
    }

    @Config("cassandra.partition-size-for-batch-select")
    public CassandraClientConfig setPartitionSizeForBatchSelect(int partitionSizeForBatchSelect)
    {
        this.partitionSizeForBatchSelect = partitionSizeForBatchSelect;
        return this;
    }
}
