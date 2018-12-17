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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.spi.type.TimeZoneKey;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
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
import static java.util.concurrent.TimeUnit.SECONDS;

public class StorageConfig
{
    private File dataDirectory;
    private DataSize minAvailableSpace = new DataSize(0, BYTE);
    private Duration chunkRecoveryTimeout = new Duration(30, SECONDS);
    private DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);
    private DataSize orcMaxReadSize = new DataSize(8, MEGABYTE);
    private DataSize orcStreamBufferSize = new DataSize(8, MEGABYTE);
    private DataSize orcTinyStripeThreshold = new DataSize(8, MEGABYTE);
    private int deletionThreads = max(1, getRuntime().availableProcessors() / 2);
    private int organizationThreads = 5;
    private boolean compactionEnabled = true;
    private Duration compactionInterval = new Duration(1, TimeUnit.HOURS);

    private long maxChunkRows = 1_000_000;
    private DataSize maxChunkSize = new DataSize(256, MEGABYTE);
    private DataSize maxBufferSize = new DataSize(256, MEGABYTE);
    private String chunkDayBoundaryTimeZone = TimeZoneKey.UTC_KEY.getId();

    private boolean organizationEnabled;
    private Duration organizationDiscoveryInterval = new Duration(6, TimeUnit.HOURS);
    private Duration organizationInterval = new Duration(7, TimeUnit.DAYS);

    @NotNull
    public File getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("storage.data-directory")
    @ConfigDescription("Base directory to use for storing chunk data")
    public StorageConfig setDataDirectory(File dataDirectory)
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
    public StorageConfig setMinAvailableSpace(DataSize minAvailableSpace)
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
    public StorageConfig setOrcMaxMergeDistance(DataSize orcMaxMergeDistance)
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
    public StorageConfig setOrcMaxReadSize(DataSize orcMaxReadSize)
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
    public StorageConfig setOrcStreamBufferSize(DataSize orcStreamBufferSize)
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
    public StorageConfig setOrcTinyStripeThreshold(DataSize orcTinyStripeThreshold)
    {
        this.orcTinyStripeThreshold = orcTinyStripeThreshold;
        return this;
    }

    @Min(1)
    public int getDeletionThreads()
    {
        return deletionThreads;
    }

    @Config("storage.max-deletion-threads")
    @ConfigDescription("Maximum number of threads to use for deletions")
    public StorageConfig setDeletionThreads(int deletionThreads)
    {
        this.deletionThreads = deletionThreads;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getChunkRecoveryTimeout()
    {
        return chunkRecoveryTimeout;
    }

    @Config("storage.chunk-recovery-timeout")
    @ConfigDescription("Maximum time to wait for a chunk to recover from chunk store while running a query")
    public StorageConfig setChunkRecoveryTimeout(Duration chunkRecoveryTimeout)
    {
        this.chunkRecoveryTimeout = chunkRecoveryTimeout;
        return this;
    }

    @Min(1)
    @Max(1_000_000_000)
    public long getMaxChunkRows()
    {
        return maxChunkRows;
    }

    @Config("storage.max-chunk-rows")
    @ConfigDescription("Approximate maximum number of rows per chunk")
    public StorageConfig setMaxChunkRows(long maxChunkRows)
    {
        this.maxChunkRows = maxChunkRows;
        return this;
    }

    @MinDataSize("1MB")
    @MaxDataSize("1GB")
    public DataSize getMaxChunkSize()
    {
        return maxChunkSize;
    }

    @Config("storage.max-chunk-size")
    @ConfigDescription("Approximate maximum uncompressed size of a chunk")
    public StorageConfig setMaxChunkSize(DataSize maxChunkSize)
    {
        this.maxChunkSize = maxChunkSize;
        return this;
    }

    @MinDataSize("1MB")
    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    @Config("storage.max-buffer-size")
    @ConfigDescription("Maximum data to buffer before flushing to disk")
    public StorageConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    @MinDuration("1s")
    public Duration getCompactionInterval()
    {
        return compactionInterval;
    }

    @Config("storage.compaction-interval")
    @ConfigDescription("How often to check for local shards that need compaction")
    public StorageConfig setCompactionInterval(Duration compactionInterval)
    {
        this.compactionInterval = compactionInterval;
        return this;
    }

    @LegacyConfig("storage.max-compaction-threads")
    @Config("storage.max-organization-threads")
    @ConfigDescription("Maximum number of threads to use for organization")
    public StorageConfig setOrganizationThreads(int organizationThreads)
    {
        this.organizationThreads = organizationThreads;
        return this;
    }

    @Min(1)
    public int getOrganizationThreads()
    {
        return organizationThreads;
    }

    public boolean isCompactionEnabled()
    {
        return compactionEnabled;
    }

    @Config("storage.compaction-enabled")
    public StorageConfig setCompactionEnabled(boolean compactionEnabled)
    {
        this.compactionEnabled = compactionEnabled;
        return this;
    }

    public DateTimeZone getChunkDayBoundaryTimeZone()
    {
        return DateTimeZone.forTimeZone(TimeZone.getTimeZone(chunkDayBoundaryTimeZone));
    }

    @Config("storage.chunk-day-boundary-time-zone")
    @ConfigDescription("Time zone to use for computing day boundary for chunk")
    public StorageConfig setChunkDayBoundaryTimeZone(String timeZone)
    {
        this.chunkDayBoundaryTimeZone = timeZone;
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
    public StorageConfig setOrganizationInterval(Duration organizationInterval)
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
    public StorageConfig setOrganizationDiscoveryInterval(Duration organizationDiscoveryInterval)
    {
        this.organizationDiscoveryInterval = organizationDiscoveryInterval;
        return this;
    }

    public boolean isOrganizationEnabled()
    {
        return organizationEnabled;
    }

    @Config("storage.organization-enabled")
    public StorageConfig setOrganizationEnabled(boolean organizationEnabled)
    {
        this.organizationEnabled = organizationEnabled;
        return this;
    }
}
