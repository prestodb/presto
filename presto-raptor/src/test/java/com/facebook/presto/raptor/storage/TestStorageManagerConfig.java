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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestStorageManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StorageManagerConfig.class)
                .setDataDirectory(null)
                .setMinAvailableSpace(new DataSize(0, BYTE))
                .setOrcMaxMergeDistance(new DataSize(1, MEGABYTE))
                .setOrcMaxReadSize(new DataSize(8, MEGABYTE))
                .setOrcStreamBufferSize(new DataSize(8, MEGABYTE))
                .setOrcTinyStripeThreshold(new DataSize(8, MEGABYTE))
                .setOrcLazyReadSmallRanges(true)
                .setDeletionThreads(max(1, getRuntime().availableProcessors() / 2))
                .setShardRecoveryTimeout(new Duration(30, SECONDS))
                .setMissingShardDiscoveryInterval(new Duration(5, MINUTES))
                .setCompactionInterval(new Duration(1, HOURS))
                .setShardEjectorInterval(new Duration(4, HOURS))
                .setRecoveryThreads(10)
                .setOrganizationThreads(5)
                .setCompactionEnabled(true)
                .setOrganizationEnabled(true)
                .setOrganizationInterval(new Duration(7, DAYS))
                .setOrganizationDiscoveryInterval(new Duration(6, HOURS))
                .setMaxShardRows(1_000_000)
                .setMaxShardSize(new DataSize(256, MEGABYTE))
                .setMaxBufferSize(new DataSize(256, MEGABYTE))
                .setOneSplitPerBucketThreshold(0)
                .setShardDayBoundaryTimeZone(TimeZoneKey.UTC_KEY.getId()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("storage.data-directory", "/data")
                .put("storage.min-available-space", "123GB")
                .put("storage.orc.max-merge-distance", "16kB")
                .put("storage.orc.max-read-size", "16kB")
                .put("storage.orc.stream-buffer-size", "16kB")
                .put("storage.orc.tiny-stripe-threshold", "15kB")
                .put("storage.orc.lazy-read-small-ranges", "false")
                .put("storage.max-deletion-threads", "999")
                .put("storage.shard-recovery-timeout", "1m")
                .put("storage.missing-shard-discovery-interval", "4m")
                .put("storage.compaction-enabled", "false")
                .put("storage.compaction-interval", "4h")
                .put("storage.organization-enabled", "false")
                .put("storage.organization-interval", "4h")
                .put("storage.organization-discovery-interval", "2h")
                .put("storage.ejector-interval", "9h")
                .put("storage.max-recovery-threads", "12")
                .put("storage.max-organization-threads", "12")
                .put("storage.max-shard-rows", "10000")
                .put("storage.max-shard-size", "10MB")
                .put("storage.max-buffer-size", "512MB")
                .put("storage.one-split-per-bucket-threshold", "4")
                .put("storage.shard-day-boundary-time-zone", "PST")
                .build();

        StorageManagerConfig expected = new StorageManagerConfig()
                .setDataDirectory(new File("/data"))
                .setMinAvailableSpace(new DataSize(123, GIGABYTE))
                .setOrcMaxMergeDistance(new DataSize(16, KILOBYTE))
                .setOrcMaxReadSize(new DataSize(16, KILOBYTE))
                .setOrcStreamBufferSize(new DataSize(16, KILOBYTE))
                .setOrcTinyStripeThreshold(new DataSize(15, KILOBYTE))
                .setOrcLazyReadSmallRanges(false)
                .setDeletionThreads(999)
                .setShardRecoveryTimeout(new Duration(1, MINUTES))
                .setMissingShardDiscoveryInterval(new Duration(4, MINUTES))
                .setCompactionEnabled(false)
                .setCompactionInterval(new Duration(4, HOURS))
                .setOrganizationEnabled(false)
                .setOrganizationInterval(new Duration(4, HOURS))
                .setOrganizationDiscoveryInterval(new Duration(2, HOURS))
                .setShardEjectorInterval(new Duration(9, HOURS))
                .setRecoveryThreads(12)
                .setOrganizationThreads(12)
                .setMaxShardRows(10_000)
                .setMaxShardSize(new DataSize(10, MEGABYTE))
                .setMaxBufferSize(new DataSize(512, MEGABYTE))
                .setOneSplitPerBucketThreshold(4)
                .setShardDayBoundaryTimeZone("PST");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidations()
    {
        assertFailsValidation(new StorageManagerConfig().setDataDirectory(null), "dataDirectory", "may not be null", NotNull.class);
    }
}
