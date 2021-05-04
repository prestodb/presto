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

import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.hive.CacheQuotaRequirement.NO_CACHE_REQUIREMENT;
import static com.facebook.presto.hive.CacheQuotaScope.PARTITION;
import static com.facebook.presto.hive.TestHivePageSink.getColumnHandles;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestHivePageSourceProvider
{
    private static final String SCHEMA_NAME = "schema";
    private static final String TABLE_NAME = "table";
    private static final String PARTITION_NAME = "partition";

    @Test
    public void testGenerateCacheQuota()
    {
        HiveClientConfig config = new HiveClientConfig();

        HiveSplit split = new HiveSplit(
                SCHEMA_NAME,
                TABLE_NAME,
                PARTITION_NAME,
                "file://test",
                0,
                10,
                10,
                Instant.now().toEpochMilli(),
                new Storage(
                        StorageFormat.create(config.getHiveStorageFormat().getSerDe(), config.getHiveStorageFormat().getInputFormat(), config.getHiveStorageFormat().getOutputFormat()),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                NO_PREFERENCE,
                getColumnHandles().size(),
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                NO_CACHE_REQUIREMENT,
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of());

        CacheQuota cacheQuota = HivePageSourceProvider.generateCacheQuota(split);
        CacheQuota expectedCacheQuota = new CacheQuota(".", Optional.empty());
        assertEquals(cacheQuota, expectedCacheQuota);

        split = new HiveSplit(
                SCHEMA_NAME,
                TABLE_NAME,
                PARTITION_NAME,
                "file://test",
                0,
                10,
                10,
                Instant.now().toEpochMilli(),
                new Storage(
                        StorageFormat.create(config.getHiveStorageFormat().getSerDe(), config.getHiveStorageFormat().getInputFormat(), config.getHiveStorageFormat().getOutputFormat()),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                NO_PREFERENCE,
                getColumnHandles().size(),
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                new CacheQuotaRequirement(PARTITION, Optional.of(DataSize.succinctDataSize(1, DataSize.Unit.MEGABYTE))),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of());

        cacheQuota = HivePageSourceProvider.generateCacheQuota(split);
        expectedCacheQuota = new CacheQuota(SCHEMA_NAME + "." + TABLE_NAME + "." + PARTITION_NAME, Optional.of(DataSize.succinctDataSize(1, DataSize.Unit.MEGABYTE)));
        assertEquals(cacheQuota, expectedCacheQuota);
    }
}
