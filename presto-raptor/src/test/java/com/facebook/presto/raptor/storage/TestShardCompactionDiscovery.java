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
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.shardInfo;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.reflect.Reflection.newProxy;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardCompactionDiscovery
{
    private static final DataSize ONE_MEGABYTE = new DataSize(1, MEGABYTE);
    private static final ReaderAttributes READER_ATTRIBUTES = new ReaderAttributes(ONE_MEGABYTE, ONE_MEGABYTE, ONE_MEGABYTE);

    private DBI dbi;
    private Handle dummyHandle;
    private File dataDir;
    private ShardManager shardManager;

    @BeforeMethod
    public void setup()
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        shardManager = createShardManager(dbi);
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
        long tableId = createTable("test", columns);
        shardManager.createTable(tableId, columns, false);
        dbi.onDemand(MetadataDao.class).updateTemporalColumnId(1, 1);

        Set<ShardInfo> nonTimeRangeShards = ImmutableSet.<ShardInfo>builder()
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .build();
        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, nonTimeRangeShards, Optional.empty());

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
        transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, timeRangeShards, Optional.empty());

        ShardCompactionManager shardCompactionManager = getShardCompactionManager();

        Set<ShardMetadata> shardMetadata = shardManager.getNodeShards("node1");
        Set<ShardMetadata> temporalMetadata = shardCompactionManager.filterShardsWithTemporalMetadata(shardMetadata, 1, 1);

        Set<UUID> actual = temporalMetadata.stream()
                .map(ShardMetadata::getShardUuid)
                .collect(toSet());

        Set<UUID> expected = timeRangeShards.stream()
                .map(ShardInfo::getShardUuid)
                .collect(toSet());

        assertEquals(actual, expected);
    }

    private ShardCompactionManager getShardCompactionManager()
    {
        StorageManager storageManager = newProxy(StorageManager.class, (proxy, method, args) -> {
            throw new UnsupportedOperationException();
        });
        return new ShardCompactionManager(
                dbi,
                "node1",
                shardManager,
                new ShardCompactor(storageManager, READER_ATTRIBUTES),
                new Duration(1, TimeUnit.HOURS),
                ONE_MEGABYTE,
                100,
                10,
                true);
    }

    private long createTable(String name, List<ColumnInfo> columns)
    {
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);
        dbi.registerMapper(new TableColumn.Mapper(new TypeRegistry()));
        long tableId = metadataDao.insertTable("test", name, true, null);
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo columnInfo = columns.get(i);
            metadataDao.insertColumn(tableId, columnInfo.getColumnId(), Long.toString(columnInfo.getColumnId()), i + 1, columnInfo.getType().toString(), i, null);
        }
        return tableId;
    }
}
