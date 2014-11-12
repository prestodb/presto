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

import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

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
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.assertFile;

public class TestOrcStorageManager
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);
    private static final DateTime EPOCH = new DateTime(0, UTC_CHRONOLOGY);
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);

    private File temporary;
    private File directory;
    private File backupDirectory;

    @BeforeClass
    public void setup()
    {
        temporary = createTempDir();
        directory = new File(temporary, "data");
        backupDirectory = new File(temporary, "backup");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary);
    }

    @Test
    public void testShardFiles()
    {
        OrcStorageManager manager = new OrcStorageManager(new File("/tmp/data"), Optional.of(new File("/tmp/backup")));

        UUID uuid = UUID.fromString("701e1a79-74f7-4f56-b438-b41e8e7d019d");

        assertEquals(
                new File("/tmp/data/storage/701/e1a/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc"),
                manager.getStorageFile(uuid));

        assertEquals(
                new File("/tmp/data/staging/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc"),
                manager.getStagingFile(uuid));

        assertEquals(
                new File("/tmp/backup/701/e1a/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc"),
                manager.getBackupFile(uuid));
    }

    @Test
    public void testWriter()
            throws Exception
    {
        OrcStorageManager manager = new OrcStorageManager(directory, Optional.of(backupDirectory));

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR);

        OutputHandle handle = manager.createOutputHandle(columnIds, columnTypes, Optional.<Long>absent());

        RowSink sink = handle.getRowSink();

        sink.beginRecord(1);
        sink.appendLong(123);
        sink.appendString("hello");
        sink.finishRecord();

        sink.beginRecord(1);
        sink.appendLong(456);
        sink.appendString("bye");
        sink.finishRecord();

        manager.commit(handle);

        UUID shardUuid = handle.getShardUuid();
        File file = manager.getStorageFile(shardUuid);
        File backupFile = manager.getBackupFile(shardUuid);

        // verify primary and backup shard exist
        assertFile(file, "primary shard");
        assertFile(backupFile, "backup shard");

        // remove primary shard to force recovery from backup
        assertTrue(file.delete());
        assertTrue(file.getParentFile().delete());
        assertFalse(file.exists());

        try (OrcDataSource dataSource = manager.openShard(shardUuid)) {
            OrcRecordReader reader = createReader(dataSource, columnIds);

            assertEquals(reader.nextBatch(), 2);

            LongVector longVector = new LongVector();
            reader.readVector(0, longVector);
            assertEquals(longVector.isNull[0], false);
            assertEquals(longVector.isNull[1], false);
            assertEquals(longVector.vector[0], 123L);
            assertEquals(longVector.vector[1], 456L);

            SliceVector stringVector = new SliceVector();
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
        OrcStorageManager manager = new OrcStorageManager(directory, Optional.<File>absent());

        List<Long> columnIds = ImmutableList.of(2L, 4L, 6L, 7L, 8L, 9L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR, VARBINARY, DATE, BOOLEAN, DOUBLE);

        byte[] bytes1 = octets(0x00, 0xFE, 0xFF);
        byte[] bytes3 = octets(0x01, 0x02, 0x19, 0x80);

        OutputHandle handle = manager.createOutputHandle(columnIds, columnTypes, Optional.<Long>absent());

        RowSink sink = handle.getRowSink();

        sink.beginRecord(1);
        sink.appendLong(123);
        sink.appendString("hello");
        sink.appendBytes(bytes1);
        sink.appendLong(dateValue(new DateTime(2001, 8, 22, 0, 0, 0, 0, UTC)));
        sink.appendBoolean(true);
        sink.appendDouble(123.45);
        sink.finishRecord();

        sink.beginRecord(1);
        for (int i = 0; i < columnIds.size(); i++) {
            sink.appendNull();
        }
        sink.finishRecord();

        sink.beginRecord(1);
        sink.appendLong(456);
        sink.appendString("bye");
        sink.appendBytes(bytes3);
        sink.appendLong(dateValue(new DateTime(2005, 4, 22, 0, 0, 0, 0, UTC)));
        sink.appendBoolean(false);
        sink.appendDouble(987.65);
        sink.finishRecord();

        manager.commit(handle);

        // no tuple domain (all)
        TupleDomain<RaptorColumnHandle> tupleDomain = TupleDomain.all();

        try (ConnectorPageSource pageSource = manager.getPageSource(handle.getShardUuid(), columnIds, columnTypes, tupleDomain)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
            assertEquals(result.getRowCount(), 3);

            MaterializedResult expected = resultBuilder(SESSION, columnTypes)
                    .row(123, "hello", sqlBinary(bytes1), sqlDate(2001, 8, 22), true, 123.45)
                    .row(null, null, null, null, null, null)
                    .row(456, "bye", sqlBinary(bytes3), sqlDate(2005, 4, 22), false, 987.65)
                    .build();

            assertEquals(result, expected);
        }

        // tuple domain within the column range
        tupleDomain = TupleDomain.withFixedValues(ImmutableMap.<RaptorColumnHandle, Comparable<?>>builder()
                .put(new RaptorColumnHandle("test", "c1", 2, BIGINT), 124L)
                .build());

        try (ConnectorPageSource pageSource = manager.getPageSource(handle.getShardUuid(), columnIds, columnTypes, tupleDomain)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
            assertEquals(result.getRowCount(), 3);
        }

        // tuple domain outside the column range
        tupleDomain = TupleDomain.withFixedValues(ImmutableMap.<RaptorColumnHandle, Comparable<?>>builder()
                .put(new RaptorColumnHandle("test", "c1", 2, BIGINT), 122L)
                .build());

        try (ConnectorPageSource pageSource = manager.getPageSource(handle.getShardUuid(), columnIds, columnTypes, tupleDomain)) {
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
        return new SqlDate(new DateTime(year, month, day, 0, 0, 0, 0, UTC).getMillis(), SESSION.getTimeZoneKey());
    }

    private static long dateValue(DateTime dateTime)
    {
        return Days.daysBetween(EPOCH, new DateTime(dateTime, UTC_CHRONOLOGY)).getDays();
    }
}
