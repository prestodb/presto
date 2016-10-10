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

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.backup.BackupManager;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.backup.FileBackupStore;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardRecorder;
import com.facebook.presto.raptor.storage.InMemoryShardRecorder.RecordedShard;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingNodeManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.chrono.ISOChronology;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.octets;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Files.hash;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.FileAssert.assertDirectory;
import static org.testng.FileAssert.assertFile;

@Test(singleThreaded = true)
public class TestOrcStorageManager
{
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);
    private static final DateTime EPOCH = new DateTime(0, UTC_CHRONOLOGY);
    private static final String CURRENT_NODE = "node";
    private static final String CONNECTOR_ID = "test";
    private static final long TRANSACTION_ID = 123;
    private static final int DELETION_THREADS = 2;
    private static final Duration SHARD_RECOVERY_TIMEOUT = new Duration(30, TimeUnit.SECONDS);
    private static final int MAX_SHARD_ROWS = 100;
    private static final DataSize MAX_FILE_SIZE = new DataSize(1, MEGABYTE);
    private static final Duration MISSING_SHARD_DISCOVERY = new Duration(5, TimeUnit.MINUTES);
    private static final ReaderAttributes READER_ATTRIBUTES = new ReaderAttributes(new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

    private final NodeManager nodeManager = new TestingNodeManager();
    private Handle dummyHandle;
    private File temporary;
    private StorageService storageService;
    private ShardRecoveryManager recoveryManager;
    private FileBackupStore fileBackupStore;
    private Optional<BackupStore> backupStore;
    private InMemoryShardRecorder shardRecorder;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        temporary = createTempDir();
        File directory = new File(temporary, "data");
        storageService = new FileStorageService(directory);
        storageService.start();

        File backupDirectory = new File(temporary, "backup");
        fileBackupStore = new FileBackupStore(backupDirectory);
        fileBackupStore.start();
        backupStore = Optional.of(fileBackupStore);

        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        ShardManager shardManager = createShardManager(dbi);
        Duration discoveryInterval = new Duration(5, TimeUnit.MINUTES);
        recoveryManager = new ShardRecoveryManager(storageService, backupStore, nodeManager, shardManager, discoveryInterval, 10);

        shardRecorder = new InMemoryShardRecorder();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (dummyHandle != null) {
            dummyHandle.close();
        }
        deleteRecursively(temporary);
    }

    @Test
    public void testWriter()
            throws Exception
    {
        OrcStorageManager manager = createOrcStorageManager();

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, createVarcharType(10));

        StoragePageSink sink = createStoragePageSink(manager, columnIds, columnTypes);
        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123L, "hello")
                .row(456L, "bye")
                .build();
        sink.appendPages(pages);

        // shard is not recorded until flush
        assertEquals(shardRecorder.getShards().size(), 0);

        sink.flush();

        // shard is recorded after flush
        List<RecordedShard> recordedShards = shardRecorder.getShards();
        assertEquals(recordedShards.size(), 1);

        List<ShardInfo> shards = getFutureValue(sink.commit());

        assertEquals(shards.size(), 1);
        ShardInfo shardInfo = Iterables.getOnlyElement(shards);

        UUID shardUuid = shardInfo.getShardUuid();
        File file = storageService.getStorageFile(shardUuid);
        File backupFile = fileBackupStore.getBackupFile(shardUuid);

        assertEquals(recordedShards.get(0).getTransactionId(), TRANSACTION_ID);
        assertEquals(recordedShards.get(0).getShardUuid(), shardUuid);

        assertEquals(shardInfo.getRowCount(), 2);
        assertEquals(shardInfo.getCompressedSize(), file.length());

        // verify primary and backup shard exist
        assertFile(file, "primary shard");
        assertFile(backupFile, "backup shard");

        assertFileEquals(file, backupFile);

        // remove primary shard to force recovery from backup
        assertTrue(file.delete());
        assertTrue(file.getParentFile().delete());
        assertFalse(file.exists());

        recoveryManager.restoreFromBackup(shardUuid, OptionalLong.empty());

        try (OrcDataSource dataSource = manager.openShard(shardUuid, READER_ATTRIBUTES)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.nextBatch(), 2);

            Block column0 = reader.readBlock(BIGINT, 0);
            assertEquals(column0.isNull(0), false);
            assertEquals(column0.isNull(1), false);
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 1), 456L);

            Block column1 = reader.readBlock(createVarcharType(10), 1);
            assertEquals(createVarcharType(10).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(10).getSlice(column1, 1), utf8Slice("bye"));

            assertEquals(reader.nextBatch(), -1);
        }
    }

    @Test
    public void testReader()
            throws Exception
    {
        OrcStorageManager manager = createOrcStorageManager();

        List<Long> columnIds = ImmutableList.of(2L, 4L, 6L, 7L, 8L, 9L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, createVarcharType(10), VARBINARY, DATE, BOOLEAN, DOUBLE);

        byte[] bytes1 = octets(0x00, 0xFE, 0xFF);
        byte[] bytes3 = octets(0x01, 0x02, 0x19, 0x80);

        StoragePageSink sink = createStoragePageSink(manager, columnIds, columnTypes);

        Object[][] doubles = {
                {881L, "-inf", null, null, null, Double.NEGATIVE_INFINITY},
                {882L, "+inf", null, null, null, Double.POSITIVE_INFINITY},
                {883L, "nan", null, null, null, Double.NaN},
                {884L, "min", null, null, null, Double.MIN_VALUE},
                {885L, "max", null, null, null, Double.MAX_VALUE},
                {886L, "pzero", null, null, null, 0.0},
                {887L, "nzero", null, null, null, -0.0},
                };

        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123L, "hello", wrappedBuffer(bytes1), sqlDate(2001, 8, 22).getDays(), true, 123.45)
                .row(null, null, null, null, null, null)
                .row(456L, "bye", wrappedBuffer(bytes3), sqlDate(2005, 4, 22).getDays(), false, 987.65)
                .rows(doubles)
                .build();

        sink.appendPages(pages);
        List<ShardInfo> shards = getFutureValue(sink.commit());

        assertEquals(shards.size(), 1);
        UUID uuid = Iterables.getOnlyElement(shards).getShardUuid();

        MaterializedResult expected = resultBuilder(SESSION, columnTypes)
                .row(123L, "hello", sqlBinary(bytes1), sqlDate(2001, 8, 22), true, 123.45)
                .row(null, null, null, null, null, null)
                .row(456L, "bye", sqlBinary(bytes3), sqlDate(2005, 4, 22), false, 987.65)
                .rows(doubles)
                .build();

        // no tuple domain (all)
        TupleDomain<RaptorColumnHandle> tupleDomain = TupleDomain.all();

        try (ConnectorPageSource pageSource = getPageSource(manager, columnIds, columnTypes, uuid, tupleDomain)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
            assertEquals(result.getRowCount(), expected.getRowCount());
            assertEquals(result, expected);
        }

        // tuple domain within the column range
        tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.<RaptorColumnHandle, NullableValue>builder()
                .put(new RaptorColumnHandle("test", "c1", 2, BIGINT), NullableValue.of(BIGINT, 124L))
                .build());

        try (ConnectorPageSource pageSource = getPageSource(manager, columnIds, columnTypes, uuid, tupleDomain)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
            assertEquals(result.getRowCount(), expected.getRowCount());
        }

        // tuple domain outside the column range
        tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.<RaptorColumnHandle, NullableValue>builder()
                .put(new RaptorColumnHandle("test", "c1", 2, BIGINT), NullableValue.of(BIGINT, 122L))
                .build());

        try (ConnectorPageSource pageSource = getPageSource(manager, columnIds, columnTypes, uuid, tupleDomain)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
            assertEquals(result.getRowCount(), 0);
        }
    }

    @Test
    public void testRewriter()
            throws Exception
    {
        OrcStorageManager manager = createOrcStorageManager();

        long transactionId = TRANSACTION_ID;
        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, createVarcharType(10));

        // create file with 2 rows
        StoragePageSink sink = createStoragePageSink(manager, columnIds, columnTypes);
        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123L, "hello")
                .row(456L, "bye")
                .build();
        sink.appendPages(pages);
        List<ShardInfo> shards = getFutureValue(sink.commit());

        assertEquals(shardRecorder.getShards().size(), 1);

        // delete one row
        BitSet rowsToDelete = new BitSet();
        rowsToDelete.set(0);
        Collection<Slice> fragments = manager.rewriteShard(transactionId, OptionalInt.empty(), shards.get(0).getShardUuid(), rowsToDelete);

        Slice shardDelta = Iterables.getOnlyElement(fragments);
        ShardDelta shardDeltas = jsonCodec(ShardDelta.class).fromJson(shardDelta.getBytes());
        ShardInfo shardInfo = Iterables.getOnlyElement(shardDeltas.getNewShards());

        // check that output file has one row
        assertEquals(shardInfo.getRowCount(), 1);

        // check that storage file is same as backup file
        File storageFile = storageService.getStorageFile(shardInfo.getShardUuid());
        File backupFile = fileBackupStore.getBackupFile(shardInfo.getShardUuid());
        assertFileEquals(storageFile, backupFile);

        // verify recorded shard
        List<RecordedShard> recordedShards = shardRecorder.getShards();
        assertEquals(recordedShards.size(), 2);
        assertEquals(recordedShards.get(1).getTransactionId(), TRANSACTION_ID);
        assertEquals(recordedShards.get(1).getShardUuid(), shardInfo.getShardUuid());
    }

    @Test
    public void testWriterRollback()
            throws Exception
    {
        // verify staging directory is empty
        File staging = new File(new File(temporary, "data"), "staging");
        assertDirectory(staging);
        assertEquals(staging.list(), new String[] {});

        // create a shard in staging
        OrcStorageManager manager = createOrcStorageManager();

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, createVarcharType(10));

        StoragePageSink sink = createStoragePageSink(manager, columnIds, columnTypes);
        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123L, "hello")
                .row(456L, "bye")
                .build();
        sink.appendPages(pages);

        sink.flush();

        // verify shard exists in staging
        String[] files = staging.list();
        assertEquals(files.length, 1);
        assertTrue(files[0].endsWith(".orc"));

        // rollback should cleanup staging files
        sink.rollback();

        assertEquals(staging.list(), new String[] {});
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
                types(createVarcharType(10)),
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
        OrcStorageManager manager = createOrcStorageManager(2, new DataSize(2, MEGABYTE));

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, createVarcharType(10));

        StoragePageSink sink = createStoragePageSink(manager, columnIds, columnTypes);
        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123L, "hello")
                .row(456L, "bye")
                .build();
        sink.appendPages(pages);
        assertTrue(sink.isFull());
    }

    @Test
    public void testMaxFileSize()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, createVarcharType(5));

        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123L, "hello")
                .row(456L, "bye")
                .build();

        // Set maxFileSize to 1 byte, so adding any page makes the StoragePageSink full
        OrcStorageManager manager = createOrcStorageManager(20, new DataSize(1, BYTE));
        StoragePageSink sink = createStoragePageSink(manager, columnIds, columnTypes);
        sink.appendPages(pages);
        assertTrue(sink.isFull());
    }

    private static ConnectorPageSource getPageSource(
            OrcStorageManager manager,
            List<Long> columnIds,
            List<Type> columnTypes,
            UUID uuid,
            TupleDomain<RaptorColumnHandle> tupleDomain)
    {
        return manager.getPageSource(uuid, OptionalInt.empty(), columnIds, columnTypes, tupleDomain, READER_ATTRIBUTES);
    }

    private static StoragePageSink createStoragePageSink(StorageManager manager, List<Long> columnIds, List<Type> columnTypes)
    {
        long transactionId = TRANSACTION_ID;
        return manager.createStoragePageSink(transactionId, OptionalInt.empty(), columnIds, columnTypes, false);
    }

    private OrcStorageManager createOrcStorageManager()
    {
        return createOrcStorageManager(MAX_SHARD_ROWS, MAX_FILE_SIZE);
    }

    private OrcStorageManager createOrcStorageManager(int maxShardRows, DataSize maxFileSize)
    {
        return createOrcStorageManager(storageService, backupStore, recoveryManager, shardRecorder, maxShardRows, maxFileSize);
    }

    public static OrcStorageManager createOrcStorageManager(IDBI dbi, File temporary)
            throws IOException
    {
        return createOrcStorageManager(dbi, temporary, MAX_SHARD_ROWS);
    }

    public static OrcStorageManager createOrcStorageManager(IDBI dbi, File temporary, int maxShardRows)
            throws IOException
    {
        File directory = new File(temporary, "data");
        StorageService storageService = new FileStorageService(directory);
        storageService.start();

        File backupDirectory = new File(temporary, "backup");
        FileBackupStore fileBackupStore = new FileBackupStore(backupDirectory);
        fileBackupStore.start();
        Optional<BackupStore> backupStore = Optional.of(fileBackupStore);

        ShardManager shardManager = createShardManager(dbi);
        ShardRecoveryManager recoveryManager = new ShardRecoveryManager(
                storageService,
                backupStore,
                new TestingNodeManager(),
                shardManager,
                MISSING_SHARD_DISCOVERY,
                10);
        return createOrcStorageManager(
                storageService,
                backupStore,
                recoveryManager,
                new InMemoryShardRecorder(),
                maxShardRows,
                MAX_FILE_SIZE);
    }

    public static OrcStorageManager createOrcStorageManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ShardRecoveryManager recoveryManager,
            ShardRecorder shardRecorder,
            int maxShardRows,
            DataSize maxFileSize)
    {
        return new OrcStorageManager(
                CURRENT_NODE,
                storageService,
                backupStore,
                SHARD_DELTA_CODEC,
                READER_ATTRIBUTES,
                new BackupManager(backupStore, 1),
                recoveryManager,
                shardRecorder,
                new TypeRegistry(),
                CONNECTOR_ID,
                DELETION_THREADS,
                SHARD_RECOVERY_TIMEOUT,
                maxShardRows,
                maxFileSize,
                new DataSize(0, BYTE));
    }

    private static void assertFileEquals(File actual, File expected)
            throws IOException
    {
        assertEquals(hash(actual, md5()), hash(expected, md5()));
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

        OrcStorageManager manager = createOrcStorageManager();
        StoragePageSink sink = createStoragePageSink(manager, columnIds, columnTypes);
        sink.appendPages(rowPagesBuilder(columnTypes).rows(rows).build());
        List<ShardInfo> shards = getFutureValue(sink.commit());

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
