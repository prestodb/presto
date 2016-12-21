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
import com.datastax.driver.core.SocketOptions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CassandraClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private Duration schemaCacheTtl = new Duration(1, TimeUnit.HOURS);
    private Duration schemaRefreshInterval = new Duration(2, TimeUnit.MINUTES);
    private int maxSchemaRefreshThreads = 10;
    private int limitForPartitionKeySelect = 200;
    private int fetchSizeForPartitionKeySelect = 20_000;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private int fetchSize = 5_000;
    private List<String> contactPoints = ImmutableList.of();
    private int nativeProtocolPort = 9042;
    private int partitionSizeForBatchSelect = 100;
    private int splitSize = 1_024;
    private String partitioner = "Murmur3Partitioner";
    private int thriftPort = 9160;
    private String thriftConnectionFactoryClassName = "org.apache.cassandra.thrift.TFramedTransportFactory";
    private Map<String, String> transportFactoryOptions = new HashMap<>();
    private boolean allowDropTable;
    private String username;
    private String password;
    private Duration clientReadTimeout = new Duration(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS, MILLISECONDS);
    private Duration clientConnectTimeout = new Duration(SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS, MILLISECONDS);
    private Integer clientSoLinger;
    private RetryPolicyType retryPolicy = RetryPolicyType.DEFAULT;
    private boolean useDCAware;
    private String dcAwareLocalDC;
    private int dcAwareUsedHostsPerRemoteDc;
    private boolean dcAwareAllowRemoteDCsForLocal;
    private boolean useTokenAware;
    private boolean tokenAwareShuffleReplicas;
    private boolean useWhiteList;
    private List<String> whiteListAddresses = ImmutableList.of();
    private int noHostAvailableRetryCount = 1;
    private int speculativeExecutionLimit = 1;
    private Duration speculativeExecutionDelay = new Duration(500, MILLISECONDS);

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

    public int getThriftPort()
    {
        return thriftPort;
    }

    @Config(("cassandra.thrift-port"))
    public CassandraClientConfig setThriftPort(int thriftPort)
    {
        this.thriftPort = thriftPort;
        return this;
    }

    @Min(1)
    public int getSplitSize()
    {
        return splitSize;
    }

    @Config("cassandra.split-size")
    public CassandraClientConfig setSplitSize(int splitSize)
    {
        this.splitSize = splitSize;
        return this;
    }

    public String getPartitioner()
    {
        return partitioner;
    }

    @Config("cassandra.partitioner")
    public CassandraClientConfig setPartitioner(String partitioner)
    {
        this.partitioner = partitioner;
        return this;
    }

    public String getThriftConnectionFactoryClassName()
    {
        return thriftConnectionFactoryClassName;
    }

    @Config("cassandra.thrift-connection-factory-class")
    public CassandraClientConfig setThriftConnectionFactoryClassName(String thriftConnectionFactoryClassName)
    {
        this.thriftConnectionFactoryClassName = thriftConnectionFactoryClassName;
        return this;
    }

    public Map<String, String> getTransportFactoryOptions()
    {
        return transportFactoryOptions;
    }

    @Config("cassandra.transport-factory-options")
    public CassandraClientConfig setTransportFactoryOptions(String transportFactoryOptions)
    {
        requireNonNull(transportFactoryOptions, "transportFactoryOptions is null");
        this.transportFactoryOptions = Splitter.on(',').omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(transportFactoryOptions);
        return this;
    }

    public boolean getAllowDropTable()
    {
        return this.allowDropTable;
    }

    @Config("cassandra.allow-drop-table")
    @ConfigDescription("Allow hive connector to drop table")
    public CassandraClientConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("cassandra.username")
    public CassandraClientConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("cassandra.password")
    public CassandraClientConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("1h")
    public Duration getClientReadTimeout()
    {
        return clientReadTimeout;
    }

    @Config("cassandra.client.read-timeout")
    public CassandraClientConfig setClientReadTimeout(Duration clientReadTimeout)
    {
        this.clientReadTimeout = clientReadTimeout;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("1h")
    public Duration getClientConnectTimeout()
    {
        return clientConnectTimeout;
    }

    @Config("cassandra.client.connect-timeout")
    public CassandraClientConfig setClientConnectTimeout(Duration clientConnectTimeout)
    {
        this.clientConnectTimeout = clientConnectTimeout;
        return this;
    }

    @Min(0)
    public Integer getClientSoLinger()
    {
        return clientSoLinger;
    }

    @Config("cassandra.client.so-linger")
    public CassandraClientConfig setClientSoLinger(Integer clientSoLinger)
    {
        this.clientSoLinger = clientSoLinger;
        return this;
    }

    @NotNull
    public RetryPolicyType getRetryPolicy()
    {
        return retryPolicy;
    }

    @Config("cassandra.retry-policy")
    public CassandraClientConfig setRetryPolicy(RetryPolicyType retryPolicy)
    {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public boolean isUseDCAware()
    {
        return this.useDCAware;
    }

    @Config("cassandra.load-policy.use-dc-aware")
    public CassandraClientConfig setUseDCAware(boolean useDCAware)
    {
        this.useDCAware = useDCAware;
        return this;
    }

    public String getDcAwareLocalDC()
    {
        return dcAwareLocalDC;
    }

    @Config("cassandra.load-policy.dc-aware.local-dc")
    public CassandraClientConfig setDcAwareLocalDC(String dcAwareLocalDC)
    {
        this.dcAwareLocalDC = dcAwareLocalDC;
        return this;
    }

    @Min(0)
    public Integer getDcAwareUsedHostsPerRemoteDc()
    {
        return dcAwareUsedHostsPerRemoteDc;
    }

    @Config("cassandra.load-policy.dc-aware.used-hosts-per-remote-dc")
    public CassandraClientConfig setDcAwareUsedHostsPerRemoteDc(Integer dcAwareUsedHostsPerRemoteDc)
    {
        this.dcAwareUsedHostsPerRemoteDc = dcAwareUsedHostsPerRemoteDc;
        return this;
    }

    public boolean isDcAwareAllowRemoteDCsForLocal()
    {
        return this.dcAwareAllowRemoteDCsForLocal;
    }

    @Config("cassandra.load-policy.dc-aware.allow-remote-dc-for-local")
    public CassandraClientConfig setDcAwareAllowRemoteDCsForLocal(boolean dcAwareAllowRemoteDCsForLocal)
    {
        this.dcAwareAllowRemoteDCsForLocal = dcAwareAllowRemoteDCsForLocal;
        return this;
    }

    public boolean isUseTokenAware()
    {
        return this.useTokenAware;
    }

    @Config("cassandra.load-policy.use-token-aware")
    public CassandraClientConfig setUseTokenAware(boolean useTokenAware)
    {
        this.useTokenAware = useTokenAware;
        return this;
    }

    public boolean isTokenAwareShuffleReplicas()
    {
        return this.tokenAwareShuffleReplicas;
    }

    @Config("cassandra.load-policy.token-aware.shuffle-replicas")
    public CassandraClientConfig setTokenAwareShuffleReplicas(boolean tokenAwareShuffleReplicas)
    {
        this.tokenAwareShuffleReplicas = tokenAwareShuffleReplicas;
        return this;
    }

    public boolean isUseWhiteList()
    {
        return this.useWhiteList;
    }

    @Config("cassandra.load-policy.use-white-list")
    public CassandraClientConfig setUseWhiteList(boolean useWhiteList)
    {
        this.useWhiteList = useWhiteList;
        return this;
    }

    public List<String> getWhiteListAddresses()
    {
        return whiteListAddresses;
    }

    @Config("cassandra.load-policy.white-list.addresses")
    public CassandraClientConfig setWhiteListAddresses(String commaSeparatedList)
    {
        this.whiteListAddresses = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    @Min(1)
    public int getNoHostAvailableRetryCount()
    {
        return noHostAvailableRetryCount;
    }

    @Config("cassandra.no-host-available-retry-count")
    public CassandraClientConfig setNoHostAvailableRetryCount(int noHostAvailableRetryCount)
    {
        this.noHostAvailableRetryCount = noHostAvailableRetryCount;
        return this;
    }

    @Min(1)
    public int getSpeculativeExecutionLimit()
    {
        return speculativeExecutionLimit;
    }

    @Config("cassandra.speculative-execution.limit")
    public CassandraClientConfig setSpeculativeExecutionLimit(int speculativeExecutionLimit)
    {
        this.speculativeExecutionLimit = speculativeExecutionLimit;
        return this;
    }

    @MinDuration("1ms")
    public Duration getSpeculativeExecutionDelay()
    {
        return speculativeExecutionDelay;
    }

    @Config("cassandra.speculative-execution.delay")
    public CassandraClientConfig setSpeculativeExecutionDelay(Duration speculativeExecutionDelay)
    {
        this.speculativeExecutionDelay = speculativeExecutionDelay;
        return this;
    }
}
