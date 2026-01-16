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
import com.facebook.airlift.configuration.LegacyConfig;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheScope;
import com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.inject.ConfigurationException;
import com.google.inject.spi.Message;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.transform;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;

public class MetastoreClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean verifyChecksum = true;
    private boolean requireHadoopNative = true;

    private Set<MetastoreCacheType> enabledCaches = ImmutableSet.of();
    private Set<MetastoreCacheType> disabledCaches = ImmutableSet.of();
    private Duration defaultMetastoreCacheTtl = new Duration(0, TimeUnit.SECONDS);
    private Map<MetastoreCacheType, Duration> perMetastoreCacheTtl = ImmutableMap.of();
    private Duration defaultMetastoreCacheRefreshInterval = new Duration(0, TimeUnit.SECONDS);
    private Map<MetastoreCacheType, Duration> perMetastoreCacheRefreshInterval = ImmutableMap.of();
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
    private int partitionCacheColumnCountLimit = 500;
    private HiveMetastoreAuthenticationType hiveMetastoreAuthenticationType = HiveMetastoreAuthenticationType.NONE;
    private boolean deleteFilesOnTableDrop;
    private boolean invalidateMetastoreCacheProcedureEnabled;

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

    public Set<MetastoreCacheType> getEnabledCaches()
    {
        return enabledCaches;
    }

    @Config("hive.metastore.cache.enabled-caches")
    @ConfigDescription("Comma-separated list of metastore cache types to enable")
    public MetastoreClientConfig setEnabledCaches(String caches)
    {
        if (caches == null) {
            this.enabledCaches = ImmutableSet.of();
            return this;
        }

        this.enabledCaches = ImmutableSet.copyOf(transform(
                SPLITTER.split(caches),
                cache -> MetastoreCacheType.valueOf(cache.toUpperCase(ENGLISH))));
        return this;
    }

    public Set<MetastoreCacheType> getDisabledCaches()
    {
        return disabledCaches;
    }

    @Config("hive.metastore.cache.disabled-caches")
    @ConfigDescription("Comma-separated list of metastore cache types to disable")
    public MetastoreClientConfig setDisabledCaches(String caches)
    {
        if (caches == null) {
            this.disabledCaches = ImmutableSet.of();
            return this;
        }

        this.disabledCaches = ImmutableSet.copyOf(transform(
                SPLITTER.split(caches),
                cache -> MetastoreCacheType.valueOf(cache.toUpperCase(ENGLISH))));
        return this;
    }

    @PostConstruct
    public void isBothEnabledAndDisabledConfigured()
    {
        if (!getEnabledCaches().isEmpty() && !getDisabledCaches().isEmpty()) {
            throw new ConfigurationException(ImmutableList.of(new Message("Only one of 'hive.metastore.cache.enabled-caches' or 'hive.metastore.cache.disabled-caches' can be set. " +
                    "These configs are mutually exclusive.")));
        }
    }

    @NotNull
    public Duration getDefaultMetastoreCacheTtl()
    {
        return defaultMetastoreCacheTtl;
    }

    @MinDuration("0ms")
    @Config("hive.metastore.cache.ttl.default")
    @ConfigDescription("Default time-to-live for Hive metastore cache entries. " +
            "It is used when no per-cache TTL override is configured")
    @LegacyConfig("hive.metastore-cache-ttl")
    public MetastoreClientConfig setDefaultMetastoreCacheTtl(Duration defaultMetastoreCacheTtl)
    {
        this.defaultMetastoreCacheTtl = defaultMetastoreCacheTtl;
        return this;
    }

    public Map<MetastoreCacheType, Duration> getPerMetastoreCacheTtl()
    {
        return perMetastoreCacheTtl;
    }

    @Config("hive.metastore.cache.per-cache-ttl")
    @ConfigDescription("Per-cache time-to-live (TTL) overrides for Hive metastore caches.\n" +
            "The value is a comma-separated list of <CACHE_TYPE>:<DURATION> pairs.")
    public MetastoreClientConfig setPerMetastoreCacheTtl(String perMetastoreCacheTtlValues)
    {
        if (perMetastoreCacheTtlValues == null || perMetastoreCacheTtlValues.isEmpty()) {
            return this;
        }

        this.perMetastoreCacheTtl = Arrays.stream(perMetastoreCacheTtlValues.split(","))
                .map(entry -> entry.split(":"))
                .filter(parts -> parts.length == 2)
                .collect(toImmutableMap(
                        parts -> MetastoreCacheType.valueOf(parts[0].trim().toUpperCase(ENGLISH)),
                        parts -> Duration.valueOf(parts[1].trim())));

        return this;
    }

    @NotNull
    public Duration getDefaultMetastoreCacheRefreshInterval()
    {
        return defaultMetastoreCacheRefreshInterval;
    }

    @MinDuration("1ms")
    @Config("hive.metastore.cache.refresh-interval.default")
    @ConfigDescription("Default refresh interval for Hive metastore cache entries.\n" +
            "Controls how often cached values are asynchronously refreshed.")
    @LegacyConfig("hive.metastore-refresh-interval")
    public MetastoreClientConfig setDefaultMetastoreCacheRefreshInterval(Duration defaultMetastoreCacheRefreshInterval)
    {
        this.defaultMetastoreCacheRefreshInterval = defaultMetastoreCacheRefreshInterval;
        return this;
    }

    public Map<MetastoreCacheType, Duration> getPerMetastoreCacheRefreshInterval()
    {
        return perMetastoreCacheRefreshInterval;
    }

    @Config("hive.metastore.cache.per-cache-refresh-interval")
    @ConfigDescription("Per-cache refresh interval overrides for Hive metastore caches.\n" +
            "The value is a comma-separated list of <CACHE_TYPE>:<DURATION> pairs.")
    public MetastoreClientConfig setPerMetastoreCacheRefreshInterval(String perMetastoreCacheRefreshIntervalValues)
    {
        if (perMetastoreCacheRefreshIntervalValues == null || perMetastoreCacheRefreshIntervalValues.isEmpty()) {
            return this;
        }

        this.perMetastoreCacheRefreshInterval = Arrays.stream(perMetastoreCacheRefreshIntervalValues.split(","))
                .map(entry -> entry.split(":"))
                .filter(parts -> parts.length == 2)
                .collect(toImmutableMap(
                        parts -> MetastoreCacheType.valueOf(parts[0].trim().toUpperCase(ENGLISH)),
                        parts -> Duration.valueOf(parts[1].trim())));

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

    public int getPartitionCacheColumnCountLimit()
    {
        return partitionCacheColumnCountLimit;
    }

    @Config("hive.partition-cache-column-count-limit")
    @ConfigDescription("The max limit on the column count for a partition to be cached")
    public MetastoreClientConfig setPartitionCacheColumnCountLimit(int partitionCacheColumnCountLimit)
    {
        this.partitionCacheColumnCountLimit = partitionCacheColumnCountLimit;
        return this;
    }

    public enum HiveMetastoreAuthenticationType
    {
        NONE,
        KERBEROS
    }

    @NotNull
    public HiveMetastoreAuthenticationType getHiveMetastoreAuthenticationType()
    {
        return hiveMetastoreAuthenticationType;
    }

    @Config("hive.metastore.authentication.type")
    @ConfigDescription("Hive Metastore authentication type")
    public MetastoreClientConfig setHiveMetastoreAuthenticationType(HiveMetastoreAuthenticationType hiveMetastoreAuthenticationType)
    {
        this.hiveMetastoreAuthenticationType = hiveMetastoreAuthenticationType;
        return this;
    }

    public boolean isDeleteFilesOnTableDrop()
    {
        return deleteFilesOnTableDrop;
    }

    @Config("hive.metastore.thrift.delete-files-on-table-drop")
    @ConfigDescription("Delete files on dropping table in case the metastore fails to do so")
    public MetastoreClientConfig setDeleteFilesOnTableDrop(boolean deleteFilesOnTableDrop)
    {
        this.deleteFilesOnTableDrop = deleteFilesOnTableDrop;
        return this;
    }

    public boolean isInvalidateMetastoreCacheProcedureEnabled()
    {
        return invalidateMetastoreCacheProcedureEnabled;
    }

    @Config("hive.invalidate-metastore-cache-procedure-enabled")
    @ConfigDescription("When enabled, users will be able to invalidate metastore cache on demand")
    public MetastoreClientConfig setInvalidateMetastoreCacheProcedureEnabled(boolean invalidateMetastoreCacheProcedureEnabled)
    {
        this.invalidateMetastoreCacheProcedureEnabled = invalidateMetastoreCacheProcedureEnabled;
        return this;
    }
}
