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

import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
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

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.octets;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.FileAssert.assertFile;

@Test(singleThreaded = true)
public class TestOrcStorageManager
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);
    private static final DateTime EPOCH = new DateTime(0, UTC_CHRONOLOGY);
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);
    private static final String CURRENT_NODE = "node";
    private static final DataSize ORC_MAX_MERGE_DISTANCE = new DataSize(1, MEGABYTE);
    private static final DataSize ORC_MAX_READ_SIZE = new DataSize(1, MEGABYTE);
    private static final DataSize ORC_STREAM_BUFFER_SIZE = new DataSize(1, MEGABYTE);
    private static final Duration SHARD_RECOVERY_TIMEOUT = new Duration(30, TimeUnit.SECONDS);
    private static final DataSize MAX_BUFFER_SIZE = new DataSize(256, MEGABYTE);
    private static final int MAX_SHARD_ROWS = 100;
    private static final DataSize MAX_FILE_SIZE = new DataSize(1, MEGABYTE);

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
        OrcStorageManager manager = createOrcStorageManager(storageService, recoveryManager);

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR);

        StoragePageSink sink = manager.createStoragePageSink(columnIds, columnTypes);
        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123, "hello")
                .row(456, "bye")
                .build();
        sink.appendPages(pages);
        List<ShardInfo> shards = sink.commit();

        assertEquals(shards.size(), 1);
        ShardInfo shardInfo = Iterables.getOnlyElement(shards);

        UUID shardUuid = shardInfo.getShardUuid();
        File file = storageService.getStorageFile(shardUuid);
        File backupFile = storageService.getBackupFile(shardUuid);

        assertEquals(shardInfo.getRowCount(), 2);
        assertEquals(shardInfo.getDataSize(), file.length());

        // verify primary and backup shard exist
        assertFile(file, "primary shard");
        assertFile(backupFile, "backup shard");

        // remove primary shard to force recovery from backup
        assertTrue(file.delete());
        assertTrue(file.getParentFile().delete());
        assertFalse(file.exists());

        recoveryManager.restoreFromBackup(shardUuid);

        try (OrcDataSource dataSource = manager.openShard(shardUuid)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

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
        OrcStorageManager manager = createOrcStorageManager(storageService, recoveryManager);

        List<Long> columnIds = ImmutableList.of(2L, 4L, 6L, 7L, 8L, 9L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR, VARBINARY, DATE, BOOLEAN, DOUBLE);

        byte[] bytes1 = octets(0x00, 0xFE, 0xFF);
        byte[] bytes3 = octets(0x01, 0x02, 0x19, 0x80);

        StoragePageSink sink = manager.createStoragePageSink(columnIds, columnTypes);

        Object[][] doubles = {
                {881, "-inf", null, null, null, Double.NEGATIVE_INFINITY},
                {882, "+inf", null, null, null, Double.POSITIVE_INFINITY},
                {883, "nan", null, null, null, Double.NaN},
                {884, "min", null, null, null, Double.MIN_VALUE},
                {885, "max", null, null, null, Double.MAX_VALUE},
                {886, "pzero", null, null, null, 0.0},
                {887, "nzero", null, null, null, -0.0},
        };

        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123, "hello", wrappedBuffer(bytes1), sqlDate(2001, 8, 22).getDays(), true, 123.45)
                .row(null, null, null, null, null, null)
                .row(456, "bye", wrappedBuffer(bytes3), sqlDate(2005, 4, 22).getDays(), false, 987.65)
                .rows(doubles)
                .build();

        sink.appendPages(pages);
        List<ShardInfo> shards = sink.commit();

        assertEquals(shards.size(), 1);
        UUID uuid = Iterables.getOnlyElement(shards).getShardUuid();

        MaterializedResult expected = resultBuilder(SESSION, columnTypes)
                .row(123, "hello", sqlBinary(bytes1), sqlDate(2001, 8, 22), true, 123.45)
                .row(null, null, null, null, null, null)
                .row(456, "bye", sqlBinary(bytes3), sqlDate(2005, 4, 22), false, 987.65)
                .rows(doubles)
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

    @Test
    public void testShardStatsBigint()
    {
        List<ColumnStats> stats = columnStats(types(BIGINT),
                row(2L),
                row(-3L),
                row(5L));
        assertColumnStats(stats, 1, -3L, 5L);
    }

    @Test
    public void testShardStatsDouble()
    {
        List<ColumnStats> stats = columnStats(types(DOUBLE),
                row(2.5),
                row(-4.1),
                row(6.6));
        assertColumnStats(stats, 1, -4.1, 6.6);
    }

    @Test
    public void testShardStatsBigintDouble()
    {
        List<ColumnStats> stats = columnStats(types(BIGINT, DOUBLE),
                row(-3L, 6.6),
                row(5L, -4.1));
        assertColumnStats(stats, 1, -3L, 5L);
        assertColumnStats(stats, 2, -4.1, 6.6);
    }

    @Test
    public void testShardStatsDoubleMinMax()
    {
        List<ColumnStats> stats = columnStats(types(DOUBLE),
                row(3.2),
                row(Double.MIN_VALUE),
                row(4.5));
        assertColumnStats(stats, 1, Double.MIN_VALUE, 4.5);

        stats = columnStats(types(DOUBLE),
                row(3.2),
                row(Double.MAX_VALUE),
                row(4.5));
        assertColumnStats(stats, 1, 3.2, Double.MAX_VALUE);
    }

    @Test
    public void testShardStatsDoubleNotFinite()
    {
        List<ColumnStats> stats = columnStats(types(DOUBLE),
                row(3.2),
                row(Double.NEGATIVE_INFINITY),
                row(4.5));
        assertColumnStats(stats, 1, null, 4.5);

        stats = columnStats(types(DOUBLE),
                row(3.2),
                row(Double.POSITIVE_INFINITY),
                row(4.5));
        assertColumnStats(stats, 1, 3.2, null);

        stats = columnStats(types(DOUBLE),
                row(3.2),
                row(Double.NaN),
                row(4.5));
        assertColumnStats(stats, 1, 3.2, 4.5);
    }

    @Test
    public void testShardStatsVarchar()
    {
        List<ColumnStats> stats = columnStats(
                types(VARCHAR),
                row(utf8Slice("hello")),
                row(utf8Slice("bye")),
                row(utf8Slice("foo")));
        assertColumnStats(stats, 1, "bye", "hello");
    }

    @Test
    public void testShardStatsBigintVarbinary()
    {
        List<ColumnStats> stats = columnStats(types(BIGINT, VARBINARY),
                row(5L, wrappedBuffer(octets(0x00))),
                row(3L, wrappedBuffer(octets(0x01))));
        assertColumnStats(stats, 1, 3L, 5L);
        assertNoColumnStats(stats, 2);
    }

    @Test
    public void testShardStatsDateTimestamp()
    {
        long minDate = sqlDate(2001, 8, 22).getDays();
        long maxDate = sqlDate(2005, 4, 22).getDays();
        long maxTimestamp = sqlTimestamp(2002, 4, 13, 6, 7, 8).getMillisUtc();
        long minTimestamp = sqlTimestamp(2001, 3, 15, 9, 10, 11).getMillisUtc();

        List<ColumnStats> stats = columnStats(types(DATE, TIMESTAMP),
                row(minDate, maxTimestamp),
                row(maxDate, minTimestamp));
        assertColumnStats(stats, 1, minDate, maxDate);
        assertColumnStats(stats, 2, minTimestamp, maxTimestamp);
    }

    @Test
    public void testMaxShardRows()
            throws Exception
    {
        OrcStorageManager manager = createOrcStorageManager(storageService, recoveryManager, 2, new DataSize(2, MEGABYTE));

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR);

        StoragePageSink sink = manager.createStoragePageSink(columnIds, columnTypes);
        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123, "hello")
                .row(456, "bye")
                .build();
        sink.appendPages(pages);
        assertTrue(sink.isFull());
    }

    @Test
    public void testMaxFileSize()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR);

        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123, "hello")
                .row(456, "bye")
                .build();
        long dataSize = 0;
        for (Page page : pages) {
            dataSize += page.getSizeInBytes();
        }

        OrcStorageManager manager = createOrcStorageManager(storageService, recoveryManager, 20, new DataSize(dataSize, BYTE));
        StoragePageSink sink = manager.createStoragePageSink(columnIds, columnTypes);
        sink.appendPages(pages);
        assertTrue(sink.isFull());
    }

    public static OrcStorageManager createOrcStorageManager(StorageService storageService, ShardRecoveryManager recoveryManager)
    {
        return createOrcStorageManager(storageService, recoveryManager, MAX_SHARD_ROWS, MAX_FILE_SIZE);
    }

    public static OrcStorageManager createOrcStorageManager(StorageService storageService, ShardRecoveryManager recoveryManager, int maxShardRows, DataSize maxFileSize)
    {
        return new OrcStorageManager(CURRENT_NODE, storageService, ORC_MAX_MERGE_DISTANCE, ORC_MAX_READ_SIZE, ORC_STREAM_BUFFER_SIZE, recoveryManager, SHARD_RECOVERY_TIMEOUT, maxShardRows, maxFileSize, MAX_BUFFER_SIZE);
    }

    private static void assertColumnStats(List<ColumnStats> list, long columnId, Object min, Object max)
    {
        for (ColumnStats stats : list) {
            if (stats.getColumnId() == columnId) {
                assertEquals(stats.getMin(), min);
                assertEquals(stats.getMax(), max);
                return;
            }
        }
        fail(format("no stats for column: %s: %s", columnId, list));
    }

    private static void assertNoColumnStats(List<ColumnStats> list, long columnId)
    {
        for (ColumnStats stats : list) {
            assertNotEquals(stats.getColumnId(), columnId);
        }
    }

    private static List<Type> types(Type... types)
    {
        return ImmutableList.copyOf(types);
    }

    private static Object[] row(Object... values)
    {
        return values;
    }

    private List<ColumnStats> columnStats(List<Type> columnTypes, Object[]... rows)
    {
        ImmutableList.Builder<Long> list = ImmutableList.builder();
        for (long i = 1; i <= columnTypes.size(); i++) {
            list.add(i);
        }
        List<Long> columnIds = list.build();

        OrcStorageManager manager = createOrcStorageManager(storageService, recoveryManager);
        StoragePageSink sink = manager.createStoragePageSink(columnIds, columnTypes);
        sink.appendPages(rowPagesBuilder(columnTypes).rows(rows).build());
        List<ShardInfo> shards = sink.commit();

        assertEquals(shards.size(), 1);
        return Iterables.getOnlyElement(shards).getColumnStats();
    }

    private static SqlVarbinary sqlBinary(byte[] bytes)
    {
        return new SqlVarbinary(bytes);
    }

    private static SqlDate sqlDate(int year, int month, int day)
    {
        DateTime date = new DateTime(year, month, day, 0, 0, 0, 0, UTC);
        return new SqlDate(Days.daysBetween(EPOCH, date).getDays());
    }

    private static SqlTimestamp sqlTimestamp(int year, int month, int day, int hour, int minute, int second)
    {
        DateTime dateTime = new DateTime(year, month, day, hour, minute, second, 0, UTC);
        return new SqlTimestamp(dateTime.getMillis(), UTC_KEY);
    }
}
