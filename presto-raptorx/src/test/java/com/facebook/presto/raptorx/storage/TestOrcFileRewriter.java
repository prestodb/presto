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
import com.facebook.presto.raptorx.storage.OrcFileRewriter.OrcFileInfo;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.raptorx.storage.CompressionType.LZ4;
import static com.facebook.presto.raptorx.storage.OrcFileRewriter.rewriteOrcFile;
import static com.facebook.presto.raptorx.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptorx.storage.OrcTestingUtil.fileOrcDataSource;
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
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestOrcFileRewriter
{
    private static final ReaderAttributes READER_ATTRIBUTES = new ReaderAttributes(new StorageConfig());
    private static final TypeManager TYPE_MANAGER = new TestingConnectorContext().getTypeManager();
    private static final OrcWriterStats STATS = new OrcWriterStats();

    private File temporary;

    @BeforeClass
    public void setup()
    {
        temporary = createTempDir();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testRewrite()
            throws Exception
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        ArrayType arrayOfArrayType = new ArrayType(arrayType);
        Type mapType = mapType(createVarcharType(5), BOOLEAN);
        Map<Long, Type> columns = ImmutableMap.<Long, Type>builder()
                .put(3L, BIGINT)
                .put(7L, createVarcharType(20))
                .put(9L, arrayType)
                .put(10L, mapType)
                .put(11L, arrayOfArrayType)
                .build();
        List<Long> columnIds = ImmutableList.copyOf(columns.keySet());
        List<Type> columnTypes = ImmutableList.copyOf(columns.values());
        long chunkId = 42;

        File file = new File(temporary, randomUUID().toString());
        try (OrcFileWriter writer = new OrcFileWriter(chunkId, columnIds, columnTypes, LZ4, file, TYPE_MANAGER, STATS)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(123L,
                            "hello",
                            arrayBlockOf(BIGINT, 1, 2),
                            mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true),
                            arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)))
                    .row(777L,
                            "sky",
                            arrayBlockOf(BIGINT, 3, 4),
                            mapBlockOf(createVarcharType(5), BOOLEAN, "k2", false),
                            arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 6)))
                    .row(456L,
                            "bye",
                            arrayBlockOf(BIGINT, 5, 6),
                            mapBlockOf(createVarcharType(5), BOOLEAN, "k3", true),
                            arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7)))
                    .row(888L,
                            "world",
                            arrayBlockOf(BIGINT, 7, 8),
                            mapBlockOf(createVarcharType(5), BOOLEAN, "k4", true),
                            arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 8), null))
                    .row(999L,
                            "done",
                            arrayBlockOf(BIGINT, 9, 10),
                            mapBlockOf(createVarcharType(5), BOOLEAN, "k5", true),
                            arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 9, 10)))
                    .build();
            writer.appendPages(pages);
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.getReaderRowCount(), 5);
            assertEquals(reader.getFileRowCount(), 5);
            assertEquals(reader.getSplitLength(), file.length());

            assertEquals(reader.nextBatch(), 5);

            Block column0 = reader.readBlock(BIGINT, 0);
            assertEquals(column0.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertFalse(column0.isNull(i));
            }
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 1), 777L);
            assertEquals(BIGINT.getLong(column0, 2), 456L);
            assertEquals(BIGINT.getLong(column0, 3), 888L);
            assertEquals(BIGINT.getLong(column0, 4), 999L);

            Block column1 = reader.readBlock(createVarcharType(20), 1);
            assertEquals(column1.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertFalse(column1.isNull(i));
            }
            assertEquals(createVarcharType(20).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(20).getSlice(column1, 1), utf8Slice("sky"));
            assertEquals(createVarcharType(20).getSlice(column1, 2), utf8Slice("bye"));
            assertEquals(createVarcharType(20).getSlice(column1, 3), utf8Slice("world"));
            assertEquals(createVarcharType(20).getSlice(column1, 4), utf8Slice("done"));

            Block column2 = reader.readBlock(arrayType, 2);
            assertEquals(column2.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertFalse(column2.isNull(i));
            }
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 0), arrayBlockOf(BIGINT, 1, 2)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 1), arrayBlockOf(BIGINT, 3, 4)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 2), arrayBlockOf(BIGINT, 5, 6)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 3), arrayBlockOf(BIGINT, 7, 8)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 4), arrayBlockOf(BIGINT, 9, 10)));

            Block column3 = reader.readBlock(mapType, 3);
            assertEquals(column3.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertFalse(column3.isNull(i));
            }
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 0), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 1), mapBlockOf(createVarcharType(5), BOOLEAN, "k2", false)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 2), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", true)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 3), mapBlockOf(createVarcharType(5), BOOLEAN, "k4", true)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 4), mapBlockOf(createVarcharType(5), BOOLEAN, "k5", true)));

            Block column4 = reader.readBlock(arrayOfArrayType, 4);
            assertEquals(column4.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertFalse(column4.isNull(i));
            }
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 1), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 6))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 2), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 3), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 8), null)));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 4), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 9, 10))));

            assertEquals(reader.nextBatch(), -1);

            OrcFileMetadata metadata = OrcFileMetadata.from(reader.getUserMetadata());
            assertEquals(metadata.getChunkId(), chunkId);
            assertEquals(metadata.getColumnTypes(), ImmutableMap.<Long, TypeSignature>builder()
                    .put(3L, BIGINT.getTypeSignature())
                    .put(7L, createVarcharType(20).getTypeSignature())
                    .put(9L, arrayType.getTypeSignature())
                    .put(10L, mapType.getTypeSignature())
                    .put(11L, arrayOfArrayType.getTypeSignature())
                    .build());
        }

        BitSet rowsToDelete = new BitSet(5);
        rowsToDelete.set(1);
        rowsToDelete.set(3);
        rowsToDelete.set(4);

        long newChunkId = 91;
        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = rewriteOrcFile(newChunkId, file, newFile, columns, LZ4, rowsToDelete, READER_ATTRIBUTES, TYPE_MANAGER, STATS);
        assertEquals(info.getRowCount(), 2);
        assertThat(info.getUncompressedSize()).isBetween(100L, 250L);

        try (OrcDataSource dataSource = fileOrcDataSource(newFile)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.getReaderRowCount(), 2);
            assertEquals(reader.getFileRowCount(), 2);
            assertEquals(reader.getSplitLength(), newFile.length());

            assertEquals(reader.nextBatch(), 2);

            Block column0 = reader.readBlock(BIGINT, 0);
            assertEquals(column0.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertFalse(column0.isNull(i));
            }
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 1), 456L);

            Block column1 = reader.readBlock(createVarcharType(20), 1);
            assertEquals(column1.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertFalse(column1.isNull(i));
            }
            assertEquals(createVarcharType(20).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(20).getSlice(column1, 1), utf8Slice("bye"));

            Block column2 = reader.readBlock(arrayType, 2);
            assertEquals(column2.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertFalse(column2.isNull(i));
            }
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 0), arrayBlockOf(BIGINT, 1, 2)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 1), arrayBlockOf(BIGINT, 5, 6)));

            Block column3 = reader.readBlock(mapType, 3);
            assertEquals(column3.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertFalse(column3.isNull(i));
            }
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 0), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 1), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", true)));

            Block column4 = reader.readBlock(arrayOfArrayType, 4);
            assertEquals(column4.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertFalse(column4.isNull(i));
            }
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 1), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7))));

            assertEquals(reader.nextBatch(), -1);

            OrcFileMetadata metadata = OrcFileMetadata.from(reader.getUserMetadata());
            assertEquals(metadata.getChunkId(), newChunkId);
            assertEquals(metadata.getColumnTypes(), transformValues(columns, Type::getTypeSignature));
        }
    }

    @Test
    public void testRewriteAllRowsDeleted()
    {
        Map<Long, Type> columns = ImmutableMap.of(3L, BIGINT);
        List<Long> columnIds = ImmutableList.of(3L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);
        long chunkId = 42;

        File file = new File(temporary, randomUUID().toString());
        try (OrcFileWriter writer = new OrcFileWriter(chunkId, columnIds, columnTypes, LZ4, file, TYPE_MANAGER, STATS)) {
            writer.appendPages(rowPagesBuilder(columnTypes).row(123L).row(456L).build());
        }

        BitSet rowsToDelete = new BitSet();
        rowsToDelete.set(0);
        rowsToDelete.set(1);

        long newChunkId = 91;
        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = rewriteOrcFile(newChunkId, file, newFile, columns, LZ4, rowsToDelete, READER_ATTRIBUTES, TYPE_MANAGER, STATS);
        assertEquals(info.getRowCount(), 0);
        assertEquals(info.getUncompressedSize(), 0);

        assertFalse(newFile.exists());
    }

    @Test
    public void testRewriteNoRowsDeleted()
            throws Exception
    {
        Map<Long, Type> columns = ImmutableMap.of(3L, BIGINT);
        List<Long> columnIds = ImmutableList.of(3L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);
        long chunkId = 42;

        File file = new File(temporary, randomUUID().toString());
        try (OrcFileWriter writer = new OrcFileWriter(chunkId, columnIds, columnTypes, LZ4, file, TYPE_MANAGER, STATS)) {
            writer.appendPages(rowPagesBuilder(columnTypes).row(123L).row(456L).build());
        }

        BitSet rowsToDelete = new BitSet();

        long newChunkId = 91;
        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = rewriteOrcFile(newChunkId, file, newFile, columns, LZ4, rowsToDelete, READER_ATTRIBUTES, TYPE_MANAGER, STATS);
        assertEquals(info.getRowCount(), 2);
        assertThat(info.getUncompressedSize()).isBetween(10L, 30L);

        try (OrcDataSource dataSource = fileOrcDataSource(newFile)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.nextBatch(), 2);

            Block column = reader.readBlock(BIGINT, 0);
            assertEquals(column.getPositionCount(), 2);
            assertFalse(column.isNull(0));
            assertFalse(column.isNull(1));
            assertEquals(BIGINT.getLong(column, 0), 123L);
            assertEquals(BIGINT.getLong(column, 1), 456L);

            assertEquals(reader.nextBatch(), -1);

            OrcFileMetadata metadata = OrcFileMetadata.from(reader.getUserMetadata());
            assertEquals(metadata.getChunkId(), newChunkId);
            assertEquals(metadata.getColumnTypes(), transformValues(columns, Type::getTypeSignature));
        }
    }

    @Test
    public void testUncompressedSize()
    {
        Map<Long, Type> columns = ImmutableMap.<Long, Type>builder()
                .put(1L, BOOLEAN)
                .put(2L, BIGINT)
                .put(3L, DOUBLE)
                .put(5L, createVarcharType(10))
                .put(6L, VARBINARY)
                .build();
        List<Long> columnIds = ImmutableList.copyOf(columns.keySet());
        List<Type> columnTypes = ImmutableList.copyOf(columns.values());
        long chunkId = 42;
        int expectedSize = 106;

        File file = new File(temporary, randomUUID().toString());
        try (OrcFileWriter writer = new OrcFileWriter(chunkId, columnIds, columnTypes, LZ4, file, TYPE_MANAGER, STATS)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(true, 123L, 98.7, "hello", utf8Slice("abc"))
                    .row(false, 456L, 65.4, "world", utf8Slice("xyz"))
                    .row(null, null, null, null, null)
                    .build();
            writer.appendPages(pages);
            assertEquals(getOnlyElement(pages).getLogicalSizeInBytes(), expectedSize);
        }

        long newChunkId = 91;
        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = rewriteOrcFile(newChunkId, file, newFile, columns, LZ4, new BitSet(), READER_ATTRIBUTES, TYPE_MANAGER, STATS);
        assertEquals(info.getRowCount(), 3);
        assertEquals(info.getUncompressedSize(), expectedSize);
    }
}
