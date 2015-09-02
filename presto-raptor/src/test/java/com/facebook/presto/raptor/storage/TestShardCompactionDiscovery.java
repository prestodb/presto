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

import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.MetadataDaoUtils;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.shardInfo;
import static com.facebook.presto.raptor.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.raptor.storage.TestShardRecovery.createShardRecoveryManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardCompactionDiscovery
{
    private IDBI dbi;
    private Handle dummyHandle;
    private File dataDir;
    private ShardManager shardManager;

    @BeforeMethod
    public void setup()
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        shardManager = new DatabaseShardManager(dbi);
    }

    @AfterMethod
    public void teardown()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
    }

    @Test
    public void testTemporalShardDiscovery()
            throws Exception
    {
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT), new ColumnInfo(2, BIGINT));
        long tableId = 1;
        shardManager.createTable(tableId, columns);
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);
        MetadataDaoUtils.createMetadataTablesWithRetry(metadataDao);
        metadataDao.updateTemporalColumnId(1, 1);

        Set<ShardInfo> nonTimeRangeShards = ImmutableSet.<ShardInfo>builder()
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .build();
        shardManager.commitShards(tableId, columns, nonTimeRangeShards, Optional.empty());

        Set<ShardInfo> timeRangeShards = ImmutableSet.<ShardInfo>builder()
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(new ColumnStats(1, 1, 10), new ColumnStats(2, 1, 10))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(new ColumnStats(1, 2, 20), new ColumnStats(2, 2, 20))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(new ColumnStats(1, 1, 10), new ColumnStats(2, 1, 10))))
                .build();
        shardManager.commitShards(tableId, columns, timeRangeShards, Optional.empty());

        StorageService storageService = new FileStorageService(dataDir);
        ShardRecoveryManager recoveryManager = createShardRecoveryManager(storageService, Optional.empty(), shardManager);
        StorageManager storageManager = createOrcStorageManager(storageService, Optional.empty(), recoveryManager);
        ShardCompactionManager shardCompactionManager = new ShardCompactionManager(
                dbi,
                "node1",
                shardManager,
                new ShardCompactor(storageManager),
                new Duration(1, TimeUnit.HOURS),
                new DataSize(1, DataSize.Unit.MEGABYTE),
                100,
                10,
                true);

        Set<ShardMetadata> shardMetadata = shardManager.getNodeTableShards("node1", 1);
        Set<ShardMetadata> temporalMetadata = shardCompactionManager.filterShardsWithTemporalMetadata(shardMetadata, 1, 1);
        assertEquals(temporalMetadata.stream().map(ShardMetadata::getShardUuid).collect(toSet()), timeRangeShards.stream().map(ShardInfo::getShardUuid).collect(toSet()));
    }
}
