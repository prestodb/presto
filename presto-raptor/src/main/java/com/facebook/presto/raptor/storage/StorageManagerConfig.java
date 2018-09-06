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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.type.TimeZoneKey;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import org.joda.time.DateTimeZone;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;

@DefunctConfig("storage.backup-directory")
public class StorageManagerConfig
{
    private File dataDirectory;
    private DataSize minAvailableSpace = new DataSize(0, BYTE);
    private Duration shardRecoveryTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration missingShardDiscoveryInterval = new Duration(5, TimeUnit.MINUTES);
    private boolean compactionEnabled = true;
    private Duration compactionInterval = new Duration(1, TimeUnit.HOURS);
    private Duration shardEjectorInterval = new Duration(4, TimeUnit.HOURS);
    private DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);
    private DataSize orcMaxReadSize = new DataSize(8, MEGABYTE);
    private DataSize orcStreamBufferSize = new DataSize(8, MEGABYTE);
    private DataSize orcTinyStripeThreshold = new DataSize(8, MEGABYTE);
    private boolean orcLazyReadSmallRanges = true;
    private int deletionThreads = max(1, getRuntime().availableProcessors() / 2);
    private int recoveryThreads = 10;
    private int organizationThreads = 5;
    private boolean organizationEnabled = true;
    private Duration organizationDiscoveryInterval = new Duration(6, TimeUnit.HOURS);
    private Duration organizationInterval = new Duration(7, TimeUnit.DAYS);

    private long maxShardRows = 1_000_000;
    private DataSize maxShardSize = new DataSize(256, MEGABYTE);
    private DataSize maxBufferSize = new DataSize(256, MEGABYTE);
    private int oneSplitPerBucketThreshold;
    private String shardDayBoundaryTimeZone = TimeZoneKey.UTC_KEY.getId();

    @NotNull
    public File getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("storage.data-directory")
    @ConfigDescription("Base directory to use for storing shard data")
    public StorageManagerConfig setDataDirectory(File dataDirectory)
    {
        this.dataDirectory = dataDirectory;
        return this;
    }

    @NotNull
    public DataSize getMinAvailableSpace()
    {
        return minAvailableSpace;
    }

    @Config("storage.min-available-space")
    @ConfigDescription("Minimum space that must be available on the data directory file system")
    public StorageManagerConfig setMinAvailableSpace(DataSize minAvailableSpace)
    {
        this.minAvailableSpace = minAvailableSpace;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxMergeDistance()
    {
        return orcMaxMergeDistance;
    }

    @Config("storage.orc.max-merge-distance")
    public StorageManagerConfig setOrcMaxMergeDistance(DataSize orcMaxMergeDistance)
    {
        this.orcMaxMergeDistance = orcMaxMergeDistance;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxReadSize()
    {
        return orcMaxReadSize;
    }

    @Config("storage.orc.max-read-size")
    public StorageManagerConfig setOrcMaxReadSize(DataSize orcMaxReadSize)
    {
        this.orcMaxReadSize = orcMaxReadSize;
        return this;
    }

    @NotNull
    public DataSize getOrcStreamBufferSize()
    {
        return orcStreamBufferSize;
    }

    @Config("storage.orc.stream-buffer-size")
    public StorageManagerConfig setOrcStreamBufferSize(DataSize orcStreamBufferSize)
    {
        this.orcStreamBufferSize = orcStreamBufferSize;
        return this;
    }

    @NotNull
    public DataSize getOrcTinyStripeThreshold()
    {
        return orcTinyStripeThreshold;
    }

    @Config("storage.orc.tiny-stripe-threshold")
    public StorageManagerConfig setOrcTinyStripeThreshold(DataSize orcTinyStripeThreshold)
    {
        this.orcTinyStripeThreshold = orcTinyStripeThreshold;
        return this;
    }

    @Deprecated
    public boolean isOrcLazyReadSmallRanges()
    {
        return orcLazyReadSmallRanges;
    }

    // TODO remove config option once efficacy is proven
    @Deprecated
    @Config("storage.orc.lazy-read-small-ranges")
    public StorageManagerConfig setOrcLazyReadSmallRanges(boolean orcLazyReadSmallRanges)
    {
        this.orcLazyReadSmallRanges = orcLazyReadSmallRanges;
        return this;
    }

    @Min(1)
    public int getDeletionThreads()
    {
        return deletionThreads;
    }

    @Config("storage.max-deletion-threads")
    @ConfigDescription("Maximum number of threads to use for deletions")
    public StorageManagerConfig setDeletionThreads(int deletionThreads)
    {
        this.deletionThreads = deletionThreads;
        return this;
    }

    @MinDuration("1s")
    public Duration getShardRecoveryTimeout()
    {
        return shardRecoveryTimeout;
    }

    @Config("storage.shard-recovery-timeout")
    @ConfigDescription("Maximum time to wait for a shard to recover from backup while running a query")
    public StorageManagerConfig setShardRecoveryTimeout(Duration shardRecoveryTimeout)
    {
        this.shardRecoveryTimeout = shardRecoveryTimeout;
        return this;
    }

    @MinDuration("1s")
    public Duration getMissingShardDiscoveryInterval()
    {
        return missingShardDiscoveryInterval;
    }

    @Config("storage.missing-shard-discovery-interval")
    @ConfigDescription("How often to check the database and local file system missing shards")
    public StorageManagerConfig setMissingShardDiscoveryInterval(Duration missingShardDiscoveryInterval)
    {
        this.missingShardDiscoveryInterval = missingShardDiscoveryInterval;
        return this;
    }

    @MinDuration("1s")
    public Duration getCompactionInterval()
    {
        return compactionInterval;
    }

    @Config("storage.compaction-interval")
    @ConfigDescription("How often to check for local shards that need compaction")
    public StorageManagerConfig setCompactionInterval(Duration compactionInterval)
    {
        this.compactionInterval = compactionInterval;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getOrganizationInterval()
    {
        return organizationInterval;
    }

    @Config("storage.organization-interval")
    @ConfigDescription("How long to wait between table organization iterations")
    public StorageManagerConfig setOrganizationInterval(Duration organizationInterval)
    {
        this.organizationInterval = organizationInterval;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getOrganizationDiscoveryInterval()
    {
        return organizationDiscoveryInterval;
    }

    @Config("storage.organization-discovery-interval")
    @ConfigDescription("How long to wait between discovering tables that need to be organized")
    public StorageManagerConfig setOrganizationDiscoveryInterval(Duration organizationDiscoveryInterval)
    {
        this.organizationDiscoveryInterval = organizationDiscoveryInterval;
        return this;
    }

    @MinDuration("5m")
    public Duration getShardEjectorInterval()
    {
        return shardEjectorInterval;
    }

    @Config("storage.ejector-interval")
    @ConfigDescription("How often to check for local shards that need ejection to balance capacity")
    public StorageManagerConfig setShardEjectorInterval(Duration shardEjectorInterval)
    {
        this.shardEjectorInterval = shardEjectorInterval;
        return this;
    }

    @Min(1)
    public int getRecoveryThreads()
    {
        return recoveryThreads;
    }

    @Config("storage.max-recovery-threads")
    @ConfigDescription("Maximum number of threads to use for recovery")
    public StorageManagerConfig setRecoveryThreads(int recoveryThreads)
    {
        this.recoveryThreads = recoveryThreads;
        return this;
    }

    @LegacyConfig("storage.max-compaction-threads")
    @Config("storage.max-organization-threads")
    @ConfigDescription("Maximum number of threads to use for organization")
    public StorageManagerConfig setOrganizationThreads(int organizationThreads)
    {
        this.organizationThreads = organizationThreads;
        return this;
    }

    @Min(1)
    public int getOrganizationThreads()
    {
        return organizationThreads;
    }

    @Min(1)
    @Max(1_000_000_000)
    public long getMaxShardRows()
    {
        return maxShardRows;
    }

    @Config("storage.max-shard-rows")
    @ConfigDescription("Approximate maximum number of rows per shard")
    public StorageManagerConfig setMaxShardRows(long maxShardRows)
    {
        this.maxShardRows = maxShardRows;
        return this;
    }

    @MinDataSize("1MB")
    @MaxDataSize("1GB")
    public DataSize getMaxShardSize()
    {
        return maxShardSize;
    }

    @Config("storage.max-shard-size")
    @ConfigDescription("Approximate maximum uncompressed size of a shard")
    public StorageManagerConfig setMaxShardSize(DataSize maxShardSize)
    {
        this.maxShardSize = maxShardSize;
        return this;
    }

    @MinDataSize("1MB")
    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    @Config("storage.max-buffer-size")
    @ConfigDescription("Maximum data to buffer before flushing to disk")
    public StorageManagerConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    public boolean isCompactionEnabled()
    {
        return compactionEnabled;
    }

    @Config("storage.compaction-enabled")
    public StorageManagerConfig setCompactionEnabled(boolean compactionEnabled)
    {
        this.compactionEnabled = compactionEnabled;
        return this;
    }

    public boolean isOrganizationEnabled()
    {
        return organizationEnabled;
    }

    @Config("storage.organization-enabled")
    public StorageManagerConfig setOrganizationEnabled(boolean organizationEnabled)
    {
        this.organizationEnabled = organizationEnabled;
        return this;
    }

    public int getOneSplitPerBucketThreshold()
    {
        return oneSplitPerBucketThreshold;
    }

    @Config("storage.one-split-per-bucket-threshold")
    @ConfigDescription("Experimental: Maximum bucket count at which to produce multiple splits per bucket")
    public StorageManagerConfig setOneSplitPerBucketThreshold(int oneSplitPerBucketThreshold)
    {
        this.oneSplitPerBucketThreshold = oneSplitPerBucketThreshold;
        return this;
    }

    public DateTimeZone getShardDayBoundaryTimeZone()
    {
        return DateTimeZone.forTimeZone(TimeZone.getTimeZone(shardDayBoundaryTimeZone));
    }

    @Config("storage.shard-day-boundary-time-zone")
    @ConfigDescription("Time zone to use for computing day boundary for shards")
    public StorageManagerConfig setShardDayBoundaryTimeZone(String timeZone)
    {
        this.shardDayBoundaryTimeZone = timeZone;
        return this;
    }
}
