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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.hive.metastore.CachingHiveMetastore.MetastoreCacheScope;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;

public class MetastoreClientConfig
{
    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean verifyChecksum = true;
    private boolean requireHadoopNative = true;

    private Duration metastoreCacheTtl = new Duration(0, TimeUnit.SECONDS);
    private Duration metastoreRefreshInterval = new Duration(0, TimeUnit.SECONDS);
    private long metastoreCacheMaximumSize = 10000;
    private long perTransactionMetastoreCacheMaximumSize = 1000;
    private int maxMetastoreRefreshThreads = 100;

    private String recordingPath;
    private boolean replay;
    private Duration recordingDuration = new Duration(0, MINUTES);
    private boolean partitionVersioningEnabled;
    private MetastoreCacheScope metastoreCacheScope = MetastoreCacheScope.ALL;
    private boolean metastoreImpersonationEnabled;
    private double partitionCacheValidationPercentage;

    public HostAndPort getMetastoreSocksProxy()
    {
        return metastoreSocksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public MetastoreClientConfig setMetastoreSocksProxy(HostAndPort metastoreSocksProxy)
    {
        this.metastoreSocksProxy = metastoreSocksProxy;
        return this;
    }

    @NotNull
    public Duration getMetastoreTimeout()
    {
        return metastoreTimeout;
    }

    @Config("hive.metastore-timeout")
    public MetastoreClientConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }

    public boolean isVerifyChecksum()
    {
        return verifyChecksum;
    }

    @Config("hive.dfs.verify-checksum")
    public MetastoreClientConfig setVerifyChecksum(boolean verifyChecksum)
    {
        this.verifyChecksum = verifyChecksum;
        return this;
    }

    @NotNull
    public Duration getMetastoreCacheTtl()
    {
        return metastoreCacheTtl;
    }

    @MinDuration("0ms")
    @Config("hive.metastore-cache-ttl")
    public MetastoreClientConfig setMetastoreCacheTtl(Duration metastoreCacheTtl)
    {
        this.metastoreCacheTtl = metastoreCacheTtl;
        return this;
    }

    @NotNull
    public Duration getMetastoreRefreshInterval()
    {
        return metastoreRefreshInterval;
    }

    @MinDuration("1ms")
    @Config("hive.metastore-refresh-interval")
    public MetastoreClientConfig setMetastoreRefreshInterval(Duration metastoreRefreshInterval)
    {
        this.metastoreRefreshInterval = metastoreRefreshInterval;
        return this;
    }

    public long getMetastoreCacheMaximumSize()
    {
        return metastoreCacheMaximumSize;
    }

    @Min(1)
    @Config("hive.metastore-cache-maximum-size")
    public MetastoreClientConfig setMetastoreCacheMaximumSize(long metastoreCacheMaximumSize)
    {
        this.metastoreCacheMaximumSize = metastoreCacheMaximumSize;
        return this;
    }

    public long getPerTransactionMetastoreCacheMaximumSize()
    {
        return perTransactionMetastoreCacheMaximumSize;
    }

    @Min(1)
    @Config("hive.per-transaction-metastore-cache-maximum-size")
    public MetastoreClientConfig setPerTransactionMetastoreCacheMaximumSize(long perTransactionMetastoreCacheMaximumSize)
    {
        this.perTransactionMetastoreCacheMaximumSize = perTransactionMetastoreCacheMaximumSize;
        return this;
    }

    @Min(1)
    public int getMaxMetastoreRefreshThreads()
    {
        return maxMetastoreRefreshThreads;
    }

    @Config("hive.metastore-refresh-max-threads")
    public MetastoreClientConfig setMaxMetastoreRefreshThreads(int maxMetastoreRefreshThreads)
    {
        this.maxMetastoreRefreshThreads = maxMetastoreRefreshThreads;
        return this;
    }

    public String getRecordingPath()
    {
        return recordingPath;
    }

    @Config("hive.metastore-recording-path")
    public MetastoreClientConfig setRecordingPath(String recordingPath)
    {
        this.recordingPath = recordingPath;
        return this;
    }

    public boolean isReplay()
    {
        return replay;
    }

    @Config("hive.replay-metastore-recording")
    public MetastoreClientConfig setReplay(boolean replay)
    {
        this.replay = replay;
        return this;
    }

    @NotNull
    public Duration getRecordingDuration()
    {
        return recordingDuration;
    }

    @Config("hive.metastore-recoding-duration")
    public MetastoreClientConfig setRecordingDuration(Duration recordingDuration)
    {
        this.recordingDuration = recordingDuration;
        return this;
    }

    public boolean isRequireHadoopNative()
    {
        return requireHadoopNative;
    }

    @Config("hive.dfs.require-hadoop-native")
    public MetastoreClientConfig setRequireHadoopNative(boolean requireHadoopNative)
    {
        this.requireHadoopNative = requireHadoopNative;
        return this;
    }

    public boolean isPartitionVersioningEnabled()
    {
        return partitionVersioningEnabled;
    }

    @Config("hive.partition-versioning-enabled")
    public MetastoreClientConfig setPartitionVersioningEnabled(boolean partitionVersioningEnabled)
    {
        this.partitionVersioningEnabled = partitionVersioningEnabled;
        return this;
    }

    @NotNull
    public MetastoreCacheScope getMetastoreCacheScope()
    {
        return metastoreCacheScope;
    }

    @Config("hive.metastore-cache-scope")
    public MetastoreClientConfig setMetastoreCacheScope(MetastoreCacheScope metastoreCacheScope)
    {
        this.metastoreCacheScope = metastoreCacheScope;
        return this;
    }

    public boolean isMetastoreImpersonationEnabled()
    {
        return metastoreImpersonationEnabled;
    }

    @Config("hive.metastore-impersonation-enabled")
    @ConfigDescription("Should Presto user be impersonated when communicating with Hive Metastore")
    public MetastoreClientConfig setMetastoreImpersonationEnabled(boolean metastoreImpersonationEnabled)
    {
        this.metastoreImpersonationEnabled = metastoreImpersonationEnabled;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("100.0")
    public double getPartitionCacheValidationPercentage()
    {
        return partitionCacheValidationPercentage;
    }

    @Config("hive.partition-cache-validation-percentage")
    public MetastoreClientConfig setPartitionCacheValidationPercentage(double partitionCacheValidationPercentage)
    {
        this.partitionCacheValidationPercentage = partitionCacheValidationPercentage;
        return this;
    }
}
