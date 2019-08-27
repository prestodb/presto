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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.OutputStreamOrcDataSink;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.Path;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.raptor.storage.FileStorageService.getFileSystemPath;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.fileOrcDataSource;
import static com.facebook.presto.raptor.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlocksEqual;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlocksEqual;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertBetweenInclusive;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.nio.file.Files.readAllBytes;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestOrcFileRewriter
{
    private static final ReaderAttributes READER_ATTRIBUTES = new ReaderAttributes(new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

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
        TypeManager typeManager = new TypeRegistry();
        // associate typeManager with a function manager
        new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());

        ArrayType arrayType = new ArrayType(BIGINT);
        ArrayType arrayOfArrayType = new ArrayType(arrayType);
        Type mapType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(createVarcharType(5).getTypeSignature()),
                TypeSignatureParameter.of(BOOLEAN.getTypeSignature())));
        List<Long> columnIds = ImmutableList.of(3L, 7L, 9L, 10L, 11L, 12L);
        DecimalType decimalType = DecimalType.createDecimalType(4, 4);

        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20), arrayType, mapType, arrayOfArrayType, decimalType);

        File file = new File(temporary, randomUUID().toString());
        try (FileWriter writer = OrcTestingUtil.createFileWriter(columnIds, columnTypes, file)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(123L, "hello", arrayBlockOf(BIGINT, 1, 2), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)), new BigDecimal("2.3"))
                    .row(777L, "sky", arrayBlockOf(BIGINT, 3, 4), mapBlockOf(createVarcharType(5), BOOLEAN, "k2", false), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 6)), new BigDecimal("2.3"))
                    .row(456L, "bye", arrayBlockOf(BIGINT, 5, 6), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7)), new BigDecimal("2.3"))
                    .row(888L, "world", arrayBlockOf(BIGINT, 7, 8), mapBlockOf(createVarcharType(5), BOOLEAN, "k4", true), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 8), null), new BigDecimal("2.3"))
                    .row(999L, "done", arrayBlockOf(BIGINT, 9, 10), mapBlockOf(createVarcharType(5), BOOLEAN, "k5", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 9, 10)), new BigDecimal("2.3"))
                    .build();
            writer.appendPages(pages);
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcBatchRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.getReaderRowCount(), 5);
            assertEquals(reader.getFileRowCount(), 5);
            assertEquals(reader.getSplitLength(), file.length());

            assertEquals(reader.nextBatch(), 5);

            Block column0 = reader.readBlock(BIGINT, 0);
            assertEquals(column0.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertEquals(column0.isNull(i), false);
            }
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 1), 777L);
            assertEquals(BIGINT.getLong(column0, 2), 456L);
            assertEquals(BIGINT.getLong(column0, 3), 888L);
            assertEquals(BIGINT.getLong(column0, 4), 999L);

            Block column1 = reader.readBlock(createVarcharType(20), 1);
            assertEquals(column1.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertEquals(column1.isNull(i), false);
            }
            assertEquals(createVarcharType(20).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(20).getSlice(column1, 1), utf8Slice("sky"));
            assertEquals(createVarcharType(20).getSlice(column1, 2), utf8Slice("bye"));
            assertEquals(createVarcharType(20).getSlice(column1, 3), utf8Slice("world"));
            assertEquals(createVarcharType(20).getSlice(column1, 4), utf8Slice("done"));

            Block column2 = reader.readBlock(arrayType, 2);
            assertEquals(column2.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertEquals(column2.isNull(i), false);
            }
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 0), arrayBlockOf(BIGINT, 1, 2)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 1), arrayBlockOf(BIGINT, 3, 4)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 2), arrayBlockOf(BIGINT, 5, 6)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 3), arrayBlockOf(BIGINT, 7, 8)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 4), arrayBlockOf(BIGINT, 9, 10)));

            Block column3 = reader.readBlock(mapType, 3);
            assertEquals(column3.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertEquals(column3.isNull(i), false);
            }
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 0), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 1), mapBlockOf(createVarcharType(5), BOOLEAN, "k2", false)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 2), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", true)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 3), mapBlockOf(createVarcharType(5), BOOLEAN, "k4", true)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 4), mapBlockOf(createVarcharType(5), BOOLEAN, "k5", true)));

            Block column4 = reader.readBlock(arrayOfArrayType, 4);
            assertEquals(column4.getPositionCount(), 5);
            for (int i = 0; i < 5; i++) {
                assertEquals(column4.isNull(i), false);
            }
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 1), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 6))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 2), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 3), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 8), null)));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 4), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 9, 10))));

            assertEquals(reader.nextBatch(), -1);

            OrcFileMetadata orcFileMetadata = METADATA_CODEC.fromJson(reader.getUserMetadata().get(OrcFileMetadata.KEY).getBytes());
            assertEquals(orcFileMetadata, new OrcFileMetadata(ImmutableMap.<Long, TypeSignature>builder()
                    .put(3L, BIGINT.getTypeSignature())
                    .put(7L, createVarcharType(20).getTypeSignature())
                    .put(9L, arrayType.getTypeSignature())
                    .put(10L, mapType.getTypeSignature())
                    .put(11L, arrayOfArrayType.getTypeSignature())
                    .put(12L, decimalType.getTypeSignature())
                    .build()));
        }

        BitSet rowsToDelete = new BitSet(5);
        rowsToDelete.set(1);
        rowsToDelete.set(3);
        rowsToDelete.set(4);

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = createFileRewriter().rewrite(getColumnTypes(columnIds, columnTypes), path(file), path(newFile), rowsToDelete);
        assertEquals(info.getRowCount(), 2);
        assertBetweenInclusive(info.getUncompressedSize(), 94L, 118L * 2);

        try (OrcDataSource dataSource = fileOrcDataSource(newFile)) {
            OrcBatchRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.getReaderRowCount(), 2);
            assertEquals(reader.getFileRowCount(), 2);
            assertEquals(reader.getSplitLength(), newFile.length());

            assertEquals(reader.nextBatch(), 2);

            Block column0 = reader.readBlock(BIGINT, 0);
            assertEquals(column0.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertEquals(column0.isNull(i), false);
            }
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 1), 456L);

            Block column1 = reader.readBlock(createVarcharType(20), 1);
            assertEquals(column1.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertEquals(column1.isNull(i), false);
            }
            assertEquals(createVarcharType(20).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(20).getSlice(column1, 1), utf8Slice("bye"));

            Block column2 = reader.readBlock(arrayType, 2);
            assertEquals(column2.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertEquals(column2.isNull(i), false);
            }
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 0), arrayBlockOf(BIGINT, 1, 2)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 1), arrayBlockOf(BIGINT, 5, 6)));

            Block column3 = reader.readBlock(mapType, 3);
            assertEquals(column3.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertEquals(column3.isNull(i), false);
            }
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 0), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true)));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column3, 1), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", true)));

            Block column4 = reader.readBlock(arrayOfArrayType, 4);
            assertEquals(column4.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertEquals(column4.isNull(i), false);
            }
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 1), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7))));

            assertEquals(reader.nextBatch(), -1);

            OrcFileMetadata orcFileMetadata = METADATA_CODEC.fromJson(reader.getUserMetadata().get(OrcFileMetadata.KEY).getBytes());
            assertEquals(orcFileMetadata, new OrcFileMetadata(ImmutableMap.<Long, TypeSignature>builder()
                    .put(3L, BIGINT.getTypeSignature())
                    .put(7L, createVarcharType(20).getTypeSignature())
                    .put(9L, arrayType.getTypeSignature())
                    .put(10L, mapType.getTypeSignature())
                    .put(11L, arrayOfArrayType.getTypeSignature())
                    .put(12L, decimalType.getTypeSignature())
                    .build()));
        }
    }

    @Test
    public void testRewriteWithoutMetadata()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20));

        File file = new File(temporary, randomUUID().toString());
        try (FileWriter writer = createFileWriter(columnIds, columnTypes, file, false)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(123L, "hello")
                    .row(777L, "sky")
                    .build();
            writer.appendPages(pages);
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcBatchRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.getReaderRowCount(), 2);
            assertEquals(reader.getFileRowCount(), 2);
            assertEquals(reader.getSplitLength(), file.length());

            assertEquals(reader.nextBatch(), 2);

            Block column0 = reader.readBlock(BIGINT, 0);
            assertEquals(column0.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertEquals(column0.isNull(i), false);
            }
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 1), 777L);

            Block column1 = reader.readBlock(createVarcharType(20), 1);
            assertEquals(column1.getPositionCount(), 2);
            for (int i = 0; i < 2; i++) {
                assertEquals(column1.isNull(i), false);
            }
            assertEquals(createVarcharType(20).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(20).getSlice(column1, 1), utf8Slice("sky"));

            assertFalse(reader.getUserMetadata().containsKey(OrcFileMetadata.KEY));
        }

        BitSet rowsToDelete = new BitSet(5);
        rowsToDelete.set(1);

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = createFileRewriter().rewrite(getColumnTypes(columnIds, columnTypes), path(file), path(newFile), rowsToDelete);
        assertEquals(info.getRowCount(), 1);
        assertBetweenInclusive(info.getUncompressedSize(), 13L, 13L * 2);

        try (OrcDataSource dataSource = fileOrcDataSource(newFile)) {
            OrcBatchRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.getReaderRowCount(), 1);
            assertEquals(reader.getFileRowCount(), 1);
            assertEquals(reader.getSplitLength(), newFile.length());

            assertEquals(reader.nextBatch(), 1);

            Block column0 = reader.readBlock(BIGINT, 0);
            assertEquals(column0.getPositionCount(), 1);
            assertEquals(column0.isNull(0), false);
            assertEquals(BIGINT.getLong(column0, 0), 123L);

            Block column1 = reader.readBlock(createVarcharType(20), 1);
            assertEquals(column1.getPositionCount(), 1);
            assertEquals(column1.isNull(0), false);
            assertEquals(createVarcharType(20).getSlice(column1, 0), utf8Slice("hello"));

            assertFalse(reader.getUserMetadata().containsKey(OrcFileMetadata.KEY));
        }
    }

    @Test
    public void testRewriteAllRowsDeleted()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);

        File file = new File(temporary, randomUUID().toString());
        try (FileWriter writer = OrcTestingUtil.createFileWriter(columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder(columnTypes).row(123L).row(456L).build());
        }

        BitSet rowsToDelete = new BitSet();
        rowsToDelete.set(0);
        rowsToDelete.set(1);

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = createFileRewriter().rewrite(getColumnTypes(columnIds, columnTypes), path(file), path(newFile), rowsToDelete);
        assertEquals(info.getRowCount(), 0);
        assertEquals(info.getUncompressedSize(), 0);

        assertFalse(newFile.exists());
    }

    @Test
    public void testRewriteNoRowsDeleted()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);

        File file = new File(temporary, randomUUID().toString());
        try (FileWriter writer = OrcTestingUtil.createFileWriter(columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder(columnTypes).row(123L).row(456L).build());
        }

        BitSet rowsToDelete = new BitSet();

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = createFileRewriter().rewrite(getColumnTypes(columnIds, columnTypes), path(file), path(newFile), rowsToDelete);
        assertEquals(info.getRowCount(), 2);
        assertBetweenInclusive(info.getUncompressedSize(), 16L, 16L * 2);
        assertEquals(readAllBytes(newFile.toPath()), readAllBytes(file.toPath()));
    }

    @Test
    public void testUncompressedSize()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(1L, 2L, 3L, 4L, 5L);
        List<Type> columnTypes = ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, createVarcharType(10), VARBINARY);

        File file = new File(temporary, randomUUID().toString());
        try (FileWriter writer = OrcTestingUtil.createFileWriter(columnIds, columnTypes, file)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(true, 123L, 98.7, "hello", utf8Slice("abc"))
                    .row(false, 456L, 65.4, "world", utf8Slice("xyz"))
                    .row(null, null, null, null, null)
                    .build();
            writer.appendPages(pages);
        }

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = createFileRewriter().rewrite(getColumnTypes(columnIds, columnTypes), path(file), path(newFile), new BitSet());
        assertEquals(info.getRowCount(), 3);
        assertBetweenInclusive(info.getUncompressedSize(), 55L, 55L * 2);
    }

    /**
     * The following test add or drop different columns
     */
    @Test
    public void testRewriterDropThenAddDifferentColumns()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(typeRegistry));
        Handle dummyHandle = dbi.open();
        File dataDir = Files.createTempDir();

        StorageManager storageManager = createOrcStorageManager(dbi, dataDir);

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20));

        File file = new File(temporary, randomUUID().toString());
        try (FileWriter writer = createFileWriter(columnIds, columnTypes, file, false)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(1L, "1")
                    .row(2L, "2")
                    .row(3L, "3")
                    .row(4L, "4")
                    .build();
            writer.appendPages(pages);
        }

        // Add a column
        File newFile1 = new File(temporary, randomUUID().toString());
        OrcFileInfo info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(3L, 7L, 10L), ImmutableList.of(BIGINT, createVarcharType(20), DOUBLE)),
                path(file),
                path(newFile1),
                new BitSet(5));
        assertEquals(info.getRowCount(), 4);
        assertEquals(readAllBytes(file.toPath()), readAllBytes(newFile1.toPath()));

        // Drop a column
        File newFile2 = new File(temporary, randomUUID().toString());
        info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(7L, 10L), ImmutableList.of(createVarcharType(20), DOUBLE)),
                path(newFile1),
                path(newFile2),
                new BitSet(5));
        assertEquals(info.getRowCount(), 4);

        // Optimized writer will keep the only column
        OrcReader orcReader = new OrcReader(fileOrcDataSource(newFile2), ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
        orcReader.getColumnNames().equals(ImmutableList.of("7"));

        // Add a column with the different ID with different type
        File newFile3 = new File(temporary, randomUUID().toString());
        info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(7L, 10L, 13L), ImmutableList.of(createVarcharType(20), DOUBLE, createVarcharType(5))),
                path(newFile2),
                path(newFile3),
                new BitSet(5));
        assertEquals(info.getRowCount(), 4);
        assertEquals(readAllBytes(newFile2.toPath()), readAllBytes(newFile3.toPath()));

        // Get prepared for the final file; make sure it is accessible from storage manager
        UUID uuid = randomUUID();
        File newFile4 = getFileSystemPath(new File(dataDir, "data/storage"), uuid);

        // Optimized ORC writer does not create the file itself
        newFile4.getParentFile().mkdirs();
        newFile4.createNewFile();

        // Drop a column and add a column; also delete 3 rows
        BitSet rowsToDelete = new BitSet(5);
        rowsToDelete.set(0);
        rowsToDelete.set(1);
        rowsToDelete.set(3);
        info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(7L, 13L, 18L), ImmutableList.of(createVarcharType(20), createVarcharType(5), INTEGER)),
                path(newFile3),
                path(newFile4),
                rowsToDelete);
        assertEquals(info.getRowCount(), 1);

        ConnectorPageSource source = storageManager.getPageSource(
                uuid,
                OptionalInt.empty(),
                ImmutableList.of(13L, 7L, 18L),
                ImmutableList.of(createVarcharType(5), createVarcharType(20), INTEGER),
                TupleDomain.all(),
                READER_ATTRIBUTES);

        Page page = null;
        while (page == null) {
            page = source.getNextPage();
        }
        assertEquals(page.getPositionCount(), 1);

        // Column 13L
        Block column0 = page.getBlock(0);
        assertTrue(column0.isNull(0));

        // Column 7L
        Block column1 = page.getBlock(1);
        assertEquals(createVarcharType(20).getSlice(column1, 0), utf8Slice("3"));

        // Column 8L
        Block column2 = page.getBlock(2);
        assertTrue(column2.isNull(0));

        // Remove all the columns
        File newFile5 = new File(temporary, randomUUID().toString());
        info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(13L, 18L), ImmutableList.of(createVarcharType(5), INTEGER)),
                path(newFile4),
                path(newFile5),
                new BitSet(5));

        // Optimized writer will drop the file
        assertEquals(info.getRowCount(), 0);
        assertFalse(newFile5.exists());

        dummyHandle.close();
        deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
    }

    /**
     * The following test drop and add the same columns; the legacy ORC rewriter will fail due to unchanged schema.
     * However, if we enforce the newly added column to always have the largest ID, this won't happen.
     */
    @Test
    public void testRewriterDropThenAddSameColumns()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(typeRegistry));
        Handle dummyHandle = dbi.open();
        File dataDir = Files.createTempDir();

        StorageManager storageManager = createOrcStorageManager(dbi, dataDir);

        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20));

        File file = new File(temporary, randomUUID().toString());
        try (FileWriter writer = createFileWriter(columnIds, columnTypes, file, false)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(2L, "2")
                    .build();
            writer.appendPages(pages);
        }

        // Add a column
        File newFile1 = new File(temporary, randomUUID().toString());
        OrcFileInfo info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(3L, 7L, 10L), ImmutableList.of(BIGINT, createVarcharType(20), DOUBLE)),
                path(file),
                path(newFile1),
                new BitSet(5));
        assertEquals(info.getRowCount(), 1);

        // Drop a column
        File newFile2 = new File(temporary, randomUUID().toString());
        info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(7L, 10L), ImmutableList.of(createVarcharType(20), DOUBLE)),
                path(newFile1),
                path(newFile2),
                new BitSet(5));
        assertEquals(info.getRowCount(), 1);

        // Add a column with the same ID but different type
        File newFile3 = new File(temporary, randomUUID().toString());
        info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(7L, 10L, 3L), ImmutableList.of(createVarcharType(20), DOUBLE, createVarcharType(5))),
                path(newFile2),
                path(newFile3),
                new BitSet(5));
        assertEquals(info.getRowCount(), 1);

        // Get prepared for the final file; make sure it is accessible from storage manager
        UUID uuid = randomUUID();
        File newFile4 = getFileSystemPath(new File(dataDir, "data/storage"), uuid);

        // Optimized ORC writer does not create the file itself
        newFile4.getParentFile().mkdirs();
        newFile4.createNewFile();

        // Drop a column and add a column
        info = createFileRewriter().rewrite(
                getColumnTypes(ImmutableList.of(7L, 3L, 8L), ImmutableList.of(createVarcharType(20), createVarcharType(5), INTEGER)),
                path(newFile3),
                path(newFile4),
                new BitSet(5));
        assertEquals(info.getRowCount(), 1);

        ConnectorPageSource source = storageManager.getPageSource(
                uuid,
                OptionalInt.empty(),
                ImmutableList.of(3L, 7L, 8L),
                ImmutableList.of(createVarcharType(5), createVarcharType(20), INTEGER),
                 TupleDomain.all(),
                READER_ATTRIBUTES);

        Page page = null;
        while (page == null) {
            page = source.getNextPage();
        }
        assertEquals(page.getPositionCount(), 1);

        try {
            // Column 3L
            Block column0 = page.getBlock(0);
            assertTrue(column0.isNull(0));

            // Column 7L
            Block column1 = page.getBlock(1);
            assertEquals(createVarcharType(20).getSlice(column1, 0), utf8Slice("2"));

            // Column 8L
            Block column2 = page.getBlock(2);
            assertTrue(column2.isNull(0));

            dummyHandle.close();
            deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
        }
        catch (UnsupportedOperationException e) {
            // Optimized ORC rewriter will respect the schema
            fail();
        }
    }

    private static FileWriter createFileWriter(List<Long> columnIds, List<Type> columnTypes, File file, boolean writeMetadata)
            throws FileNotFoundException
    {
        return new OrcFileWriter(
                columnIds,
                columnTypes,
                new OutputStreamOrcDataSink(new FileOutputStream(file)),
                writeMetadata,
                true,
                new OrcWriterStats(),
                new TypeRegistry(),
                ZSTD);
    }

    private static FileRewriter createFileRewriter()
    {
        TypeRegistry typeManager = new TypeRegistry();
        new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        return new OrcPageFileRewriter(READER_ATTRIBUTES, true, new OrcWriterStats(), typeManager, new LocalOrcDataEnvironment(), ZSTD);
    }

    private static Map<String, Type> getColumnTypes(List<Long> columnIds, List<Type> columnTypes)
    {
        return IntStream.range(0, columnIds.size()).boxed().collect(Collectors.toMap(index -> String.valueOf(columnIds.get(index)), columnTypes::get));
    }

    private static Path path(File file)
    {
        return new Path(file.toURI());
    }
}
