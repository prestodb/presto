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
import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.octets;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestCappedRowSink
{
    private static final DataSize ORC_MERGE_DISTANCE = new DataSize(1, MEGABYTE);
    private File directory;
    private Handle dummyHandle;
    private StorageService storageService;
    private StorageManager storageManager;

    @BeforeClass
    public void setup()
            throws IOException
    {
        directory = createTempDir();
        storageService = new StorageService(directory, Optional.<File>absent(), ORC_MERGE_DISTANCE);
        storageService.start();
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        ShardManager shardManager = new DatabaseShardManager(dbi);
        ShardRecoveryManager recoveryManager = new ShardRecoveryManager(storageService, new InMemoryNodeManager(), shardManager);
        storageManager = new OrcStorageManager(storageService, recoveryManager);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(directory);
        dummyHandle.close();
    }

    @Test
    public void testCappedRowSink()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(1L, 2L, 4L, 6L, 7L);
        List<Type> columnTypes = ImmutableList.<Type>of(BIGINT, VARCHAR, VARBINARY, DoubleType.DOUBLE, BooleanType.BOOLEAN);
        Optional<Long> sampleWeightColumnId = Optional.absent();

        byte[] bytes1 = octets(0x00, 0xFE, 0xFF);
        byte[] bytes3 = octets(0x01, 0x02, 0x19, 0x80);

        TupleBuffer tupleBuffer = new TupleBuffer(columnTypes, -1);
        OutputHandle handle = new OutputHandle(columnIds, columnTypes, storageService);
        try (RowSink sink = handle.getRowSink(Optional.of(2))) {
            tupleBuffer.reset();
            tupleBuffer.appendLong(123);
            tupleBuffer.appendSlice(utf8Slice("hello"));
            tupleBuffer.appendSlice(wrappedBuffer(bytes1));
            tupleBuffer.appendDouble(123.456);
            tupleBuffer.appendBoolean(true);
            sink.appendTuple(tupleBuffer);

            tupleBuffer.reset();
            tupleBuffer.appendNull();
            tupleBuffer.appendSlice(utf8Slice("world"));
            tupleBuffer.appendNull();
            tupleBuffer.appendDouble(Double.POSITIVE_INFINITY);
            tupleBuffer.appendNull();
            sink.appendTuple(tupleBuffer);

            tupleBuffer.reset();
            tupleBuffer.appendLong(456);
            tupleBuffer.appendSlice(utf8Slice("bye"));
            tupleBuffer.appendSlice(wrappedBuffer(bytes3));
            tupleBuffer.appendDouble(Double.NaN);
            tupleBuffer.appendBoolean(false);
            sink.appendTuple(tupleBuffer);

            tupleBuffer.reset();
            tupleBuffer.appendLong(456);
            tupleBuffer.appendSlice(utf8Slice("bye"));
            tupleBuffer.appendSlice(wrappedBuffer(bytes3));
            tupleBuffer.appendDouble(Double.NaN);
            tupleBuffer.appendBoolean(false);
            sink.appendTuple(tupleBuffer);
        }
        List<UUID> uuids = storageManager.commit(handle);
        assertEquals(uuids.size(), 2);
        for (UUID uuid : uuids) {
            File file = storageService.getStorageFile(uuid);
            try (FileOrcDataSource dataSource = new FileOrcDataSource(file, new DataSize(1, MEGABYTE))) {
                OrcRecordReader reader = createReader(dataSource, columnIds);
                assertEquals(reader.getTotalRowCount(), 2);
            }
        }
    }
}
