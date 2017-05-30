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

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.raptorx.storage.CompressionType.LZ4;
import static com.facebook.presto.raptorx.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptorx.storage.OrcTestingUtil.fileOrcDataSource;
import static com.facebook.presto.raptorx.storage.OrcTestingUtil.octets;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlocksEqual;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlocksEqual;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOrcFileWriter
{
    private static final TypeManager TYPE_MANAGER = new TestingConnectorContext().getTypeManager();
    private static final OrcWriterStats STATS = new OrcWriterStats();

    private File directory;

    @BeforeClass
    public void setup()
    {
        directory = createTempDir();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(directory.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testWriter()
            throws Exception
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        ArrayType arrayOfArrayType = new ArrayType(arrayType);
        Type mapType = mapType(createVarcharType(10), BOOLEAN);

        List<Long> columnIds = ImmutableList.of(1L, 2L, 4L, 6L, 7L, 8L, 9L, 10L);
        List<Type> columnTypes = ImmutableList.<Type>builder()
                .add(BIGINT)
                .add(createVarcharType(10))
                .add(VARBINARY)
                .add(DOUBLE)
                .add(BOOLEAN)
                .add(arrayType)
                .add(mapType)
                .add(arrayOfArrayType)
                .build();

        long chunkId = 42;

        File file = new File(directory, randomUUID().toString());

        byte[] bytes1 = octets(0x00, 0xFE, 0xFF);
        byte[] bytes3 = octets(0x01, 0x02, 0x19, 0x80);

        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(123L,
                        "hello",
                        wrappedBuffer(bytes1),
                        123.456,
                        true,
                        arrayBlockOf(BIGINT, 1, 2),
                        mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true),
                        arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)))
                .row(null,
                        "world",
                        null,
                        Double.POSITIVE_INFINITY,
                        null,
                        arrayBlockOf(BIGINT, 3, null),
                        mapBlockOf(createVarcharType(5), BOOLEAN, "k2", null),
                        arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 6, 7)))
                .row(456L,
                        "bye \u2603",
                        wrappedBuffer(bytes3),
                        Double.NaN,
                        false,
                        arrayBlockOf(BIGINT),
                        mapBlockOf(createVarcharType(5), BOOLEAN, "k3", false),
                        arrayBlockOf(arrayType, arrayBlockOf(BIGINT)))
                .build();

        try (OrcFileWriter writer = new OrcFileWriter(chunkId, columnIds, columnTypes, LZ4, file, TYPE_MANAGER, STATS)) {
            writer.appendPages(pages);
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);
            assertEquals(reader.getReaderRowCount(), 3);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFileRowCount(), reader.getReaderRowCount());
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            assertEquals(reader.nextBatch(), 3);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            Block column0 = reader.readBlock(BIGINT, 0);
            assertFalse(column0.isNull(0));
            assertTrue(column0.isNull(1));
            assertFalse(column0.isNull(2));
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 2), 456L);

            Block column1 = reader.readBlock(createVarcharType(10), 1);
            assertEquals(createVarcharType(10).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(10).getSlice(column1, 1), utf8Slice("world"));
            assertEquals(createVarcharType(10).getSlice(column1, 2), utf8Slice("bye \u2603"));

            Block column2 = reader.readBlock(VARBINARY, 2);
            assertEquals(VARBINARY.getSlice(column2, 0), wrappedBuffer(bytes1));
            assertTrue(column2.isNull(1));
            assertEquals(VARBINARY.getSlice(column2, 2), wrappedBuffer(bytes3));

            Block column3 = reader.readBlock(DOUBLE, 3);
            assertFalse(column3.isNull(0));
            assertFalse(column3.isNull(1));
            assertFalse(column3.isNull(2));
            assertEquals(DOUBLE.getDouble(column3, 0), 123.456);
            assertEquals(DOUBLE.getDouble(column3, 1), Double.POSITIVE_INFINITY);
            assertEquals(DOUBLE.getDouble(column3, 2), Double.NaN);

            Block column4 = reader.readBlock(BOOLEAN, 4);
            assertFalse(column4.isNull(0));
            assertTrue(column4.isNull(1));
            assertFalse(column4.isNull(2));
            assertTrue(BOOLEAN.getBoolean(column4, 0));
            assertFalse(BOOLEAN.getBoolean(column4, 2));

            Block column5 = reader.readBlock(arrayType, 5);
            assertEquals(column5.getPositionCount(), 3);

            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 0), arrayBlockOf(BIGINT, 1, 2)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 1), arrayBlockOf(BIGINT, 3, null)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 2), arrayBlockOf(BIGINT)));

            Block column6 = reader.readBlock(mapType, 6);
            assertEquals(column6.getPositionCount(), 3);

            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column6, 0), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true)));
            Block object = arrayType.getObject(column6, 1);
            Block k2 = mapBlockOf(createVarcharType(5), BOOLEAN, "k2", null);
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, object, k2));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column6, 2), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", false)));

            Block column7 = reader.readBlock(arrayOfArrayType, 7);
            assertEquals(column7.getPositionCount(), 3);

            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 1), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 6, 7))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 2), arrayBlockOf(arrayType, arrayBlockOf(BIGINT))));

            assertEquals(reader.nextBatch(), -1);
            assertEquals(reader.getReaderPosition(), 3);
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            OrcFileMetadata metadata = OrcFileMetadata.from(reader.getUserMetadata());
            assertEquals(metadata.getChunkId(), chunkId);
            assertEquals(metadata.getColumnTypes(), ImmutableMap.<Long, TypeSignature>builder()
                    .put(1L, BIGINT.getTypeSignature())
                    .put(2L, createVarcharType(10).getTypeSignature())
                    .put(4L, VARBINARY.getTypeSignature())
                    .put(6L, DOUBLE.getTypeSignature())
                    .put(7L, BOOLEAN.getTypeSignature())
                    .put(8L, arrayType.getTypeSignature())
                    .put(9L, mapType.getTypeSignature())
                    .put(10L, arrayOfArrayType.getTypeSignature())
                    .build());
        }
    }

    @Test
    public void testWriterAppendIndexed()
            throws Exception
    {
        VarcharType varcharType = createVarcharType(20);
        List<Long> columnIds = ImmutableList.of(1L, 2L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, varcharType);
        long chunkId = 42;

        File file = new File(directory, randomUUID().toString());

        List<Page> pages = rowPagesBuilder(columnTypes)
                .row(11, "one")
                .row(22, "two")
                .pageBreak()
                .row(33, "three")
                .row(44, "four")
                .row(55, "five")
                .pageBreak()
                .row(66, "six")
                .row(77, "seven")
                .build();

        try (OrcFileWriter writer = new OrcFileWriter(chunkId, columnIds, columnTypes, LZ4, file, TYPE_MANAGER, STATS)) {
            writer.appendPages(pages,
                    new int[] {1, 2, 2, 0, 0, 1, 1},
                    new int[] {0, 1, 0, 1, 0, 2, 1});
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.nextBatch(), 7);

            Block column0 = reader.readBlock(BIGINT, 0);
            assertBlockEquals(BIGINT, column0, createLongsBlock(33, 77, 66, 22, 11, 55, 44));

            Block column1 = reader.readBlock(varcharType, 1);
            assertBlockEquals(varcharType, column1, createStringsBlock("three", "seven", "six", "two", "one", "five", "four"));

            assertEquals(reader.nextBatch(), -1);

            OrcFileMetadata metadata = OrcFileMetadata.from(reader.getUserMetadata());
            assertEquals(metadata.getChunkId(), chunkId);
            assertEquals(metadata.getColumnTypes(), ImmutableMap.<Long, TypeSignature>builder()
                    .put(1L, BIGINT.getTypeSignature())
                    .put(2L, createVarcharType(20).getTypeSignature())
                    .build());
        }
    }

    @SuppressWarnings("EmptyTryBlock")
    @Test
    public void testWriterZeroRows()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(1L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);
        long chunkId = 42;

        File file = new File(directory, randomUUID().toString());

        try (OrcFileWriter ignored = new OrcFileWriter(chunkId, columnIds, columnTypes, LZ4, file, TYPE_MANAGER, STATS)) {
            // no rows
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);
            assertEquals(reader.getReaderRowCount(), 0);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.nextBatch(), -1);
        }
    }
}
