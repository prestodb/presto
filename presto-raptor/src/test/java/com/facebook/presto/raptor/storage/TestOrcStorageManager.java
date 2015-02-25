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

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.chrono.ISOChronology;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.raptor.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.octets;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.assertFile;

@Test(singleThreaded = true)
public class TestOrcStorageManager
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);
    private static final DateTime EPOCH = new DateTime(0, UTC_CHRONOLOGY);
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);
    private static final DataSize ORC_MERGE_DISTANCE = new DataSize(1, MEGABYTE);
    private static final Duration SHARD_RECOVERY_TIMEOUT = new Duration(30, TimeUnit.SECONDS);
    private static final DataSize MAX_BUFFER_SIZE = new DataSize(256, MEGABYTE);
    public static final int ROWS_PER_SHARD = 100;

    private final NodeManager nodeManager = new InMemoryNodeManager();
    private Handle dummyHandle;
    private File temporary;
    private StorageService storageService;
    private ShardRecoveryManager recoveryManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        temporary = createTempDir();
        File directory = new File(temporary, "data");
        File backupDirectory = new File(temporary, "backup");
        storageService = new FileStorageService(directory, Optional.of(backupDirectory));
        storageService.start();

        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        ShardManager shardManager = new DatabaseShardManager(dbi);
        recoveryManager = new ShardRecoveryManager(storageService, nodeManager, shardManager, new Duration(5, TimeUnit.MINUTES), 10);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (dummyHandle != null) {
            dummyHandle.close();
        }
        deleteRecursively(temporary);
    }

    @Test
    public void testShardFiles()
            throws Exception
    {
        UUID uuid = UUID.fromString("701e1a79-74f7-4f56-b438-b41e8e7d019d");

        assertEquals(
                new File(temporary, "data/storage/701/e1a/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc"),
                storageService.getStorageFile(uuid));

        assertEquals(
                new File(temporary, "data/staging/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc"),
                storageService.getStagingFile(uuid));

        assertEquals(
                new File(temporary, "backup/701/e1a/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc"),
                storageService.getBackupFile(uuid));
    }

    @Test
    public void testWriter()
            throws Exception
    {
        OrcStorageManager manager = new OrcStorageManager(storageService, ORC_MERGE_DISTANCE, recoveryManager, SHARD_RECOVERY_TIMEOUT, ROWS_PER_SHARD, MAX_BUFFER_SIZE);

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR);

        StoragePageSink sink = manager.createStoragePageSink(columnIds, columnTypes);
        List<Page> pages = RowPagesBuilder.rowPagesBuilder(columnTypes)
                .row(123, "hello")
                .row(456, "bye")
                .build();
        sink.appendPages(pages);
        List<UUID> uuids = sink.commit();

        assertEquals(uuids.size(), 1);
        UUID shardUuid = Iterables.getOnlyElement(uuids);
        File file = storageService.getStorageFile(shardUuid);
        File backupFile = storageService.getBackupFile(shardUuid);

        // verify primary and backup shard exist
        assertFile(file, "primary shard");
        assertFile(backupFile, "backup shard");

        // remove primary shard to force recovery from backup
        assertTrue(file.delete());
        assertTrue(file.getParentFile().delete());
        assertFalse(file.exists());

        recoveryManager.restoreFromBackup(shardUuid);

        try (OrcDataSource dataSource = manager.openShard(shardUuid)) {
            OrcRecordReader reader = createReader(dataSource, columnIds);

            assertEquals(reader.nextBatch(), 2);

            LongVector longVector = new LongVector(2);
            reader.readVector(0, longVector);
            assertEquals(longVector.isNull[0], false);
            assertEquals(longVector.isNull[1], false);
            assertEquals(longVector.vector[0], 123L);
            assertEquals(longVector.vector[1], 456L);

            SliceVector stringVector = new SliceVector(2);
            reader.readVector(1, stringVector);
            assertEquals(stringVector.vector[0], utf8Slice("hello"));
            assertEquals(stringVector.vector[1], utf8Slice("bye"));

            assertEquals(reader.nextBatch(), -1);
        }
    }

    @Test
    public void testReader()
            throws Exception
    {
        OrcStorageManager manager = new OrcStorageManager(storageService, ORC_MERGE_DISTANCE, recoveryManager, SHARD_RECOVERY_TIMEOUT, ROWS_PER_SHARD, MAX_BUFFER_SIZE);

        List<Long> columnIds = ImmutableList.of(2L, 4L, 6L, 7L, 8L, 9L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR, VARBINARY, DATE, BOOLEAN, DOUBLE);

        byte[] bytes1 = octets(0x00, 0xFE, 0xFF);
        byte[] bytes3 = octets(0x01, 0x02, 0x19, 0x80);

        StoragePageSink sink = manager.createStoragePageSink(columnIds, columnTypes);

        List<Page> pages = RowPagesBuilder.rowPagesBuilder(columnTypes)
                .row(123, "hello", wrappedBuffer(bytes1), dateValue(new DateTime(2001, 8, 22, 0, 0, 0, 0, UTC)), true, 123.45)
                .row(null, null, null, null, null, null)
                .row(456, "bye", wrappedBuffer(bytes3), dateValue(new DateTime(2005, 4, 22, 0, 0, 0, 0, UTC)), false, 987.65)
                .row(881, "-inf", null, null, null, Double.NEGATIVE_INFINITY)
                .row(882, "+inf", null, null, null, Double.POSITIVE_INFINITY)
                .row(883, "nan", null, null, null, Double.NaN)
                .row(884, "min", null, null, null, Double.MIN_VALUE)
                .row(885, "max", null, null, null, Double.MAX_VALUE)
                .build();

        sink.appendPages(pages);
        List<UUID> uuids = sink.commit();

        assertEquals(uuids.size(), 1);
        UUID uuid = Iterables.getOnlyElement(uuids);

        MaterializedResult expected = resultBuilder(SESSION, columnTypes)
                .row(123, "hello", sqlBinary(bytes1), sqlDate(2001, 8, 22), true, 123.45)
                .row(null, null, null, null, null, null)
                .row(456, "bye", sqlBinary(bytes3), sqlDate(2005, 4, 22), false, 987.65)
                .row(881, "-inf", null, null, null, Double.NEGATIVE_INFINITY)
                .row(882, "+inf", null, null, null, Double.POSITIVE_INFINITY)
                .row(883, "nan", null, null, null, Double.NaN)
                .row(884, "min", null, null, null, Double.MIN_VALUE)
                .row(885, "max", null, null, null, Double.MAX_VALUE)
                .build();

        // no tuple domain (all)
        TupleDomain<RaptorColumnHandle> tupleDomain = TupleDomain.all();

        try (ConnectorPageSource pageSource = manager.getPageSource(uuid, columnIds, columnTypes, tupleDomain)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
            assertEquals(result.getRowCount(), expected.getRowCount());
            assertEquals(result, expected);
        }

        // tuple domain within the column range
        tupleDomain = TupleDomain.withFixedValues(ImmutableMap.<RaptorColumnHandle, Comparable<?>>builder()
                .put(new RaptorColumnHandle("test", "c1", 2, BIGINT), 124L)
                .build());

        try (ConnectorPageSource pageSource = manager.getPageSource(uuid, columnIds, columnTypes, tupleDomain)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
            assertEquals(result.getRowCount(), expected.getRowCount());
        }

        // tuple domain outside the column range
        tupleDomain = TupleDomain.withFixedValues(ImmutableMap.<RaptorColumnHandle, Comparable<?>>builder()
                .put(new RaptorColumnHandle("test", "c1", 2, BIGINT), 122L)
                .build());

        try (ConnectorPageSource pageSource = manager.getPageSource(uuid, columnIds, columnTypes, tupleDomain)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
            assertEquals(result.getRowCount(), 0);
        }
    }

    private static SqlVarbinary sqlBinary(byte[] bytes)
    {
        return new SqlVarbinary(bytes);
    }

    private static SqlDate sqlDate(int year, int month, int day)
    {
        return new SqlDate(dateValue(new DateTime(year, month, day, 0, 0, 0, 0, UTC)));
    }

    private static int dateValue(DateTime dateTime)
    {
        return Days.daysBetween(EPOCH, new DateTime(dateTime, UTC_CHRONOLOGY)).getDays();
    }
}
