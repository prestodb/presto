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
import com.facebook.presto.raptor.storage.OrcFileRewriter.OrcFileInfo;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.BitSet;
import java.util.List;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.fileOrcDataSource;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Files.toByteArray;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestOrcFileRewriter
{
    private File temporary;

    @BeforeClass
    public void setup()
            throws Exception
    {
        temporary = createTempDir();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary);
    }

    @Test
    public void testRewrite()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, VARCHAR);

        File file = new File(temporary, randomUUID().toString());
        try (OrcFileWriter writer = new OrcFileWriter(columnIds, columnTypes, file)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(123, "hello")
                    .row(777, "sky")
                    .row(456, "bye")
                    .row(888, "world")
                    .row(999, "done")
                    .build();
            writer.appendPages(pages);
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.getTotalRowCount(), 5);
            assertEquals(reader.getFileRowCount(), 5);
            assertEquals(reader.getSplitLength(), file.length());

            assertEquals(reader.nextBatch(), 5);

            LongVector longVector = new LongVector(5);
            reader.readVector(0, longVector);
            for (int i = 0; i < 5; i++) {
                assertEquals(longVector.isNull[i], false);
            }
            assertEquals(longVector.vector[0], 123L);
            assertEquals(longVector.vector[1], 777L);
            assertEquals(longVector.vector[2], 456L);
            assertEquals(longVector.vector[3], 888L);
            assertEquals(longVector.vector[4], 999L);

            SliceVector stringVector = new SliceVector(5);
            reader.readVector(1, stringVector);
            assertEquals(stringVector.vector[0], utf8Slice("hello"));
            assertEquals(stringVector.vector[1], utf8Slice("sky"));
            assertEquals(stringVector.vector[2], utf8Slice("bye"));
            assertEquals(stringVector.vector[3], utf8Slice("world"));
            assertEquals(stringVector.vector[4], utf8Slice("done"));

            assertEquals(reader.nextBatch(), -1);
        }

        BitSet rowsToDelete = new BitSet(5);
        rowsToDelete.set(1);
        rowsToDelete.set(3);
        rowsToDelete.set(4);

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = OrcFileRewriter.rewrite(file, newFile, rowsToDelete);
        assertEquals(info.getRowCount(), 2);
        assertEquals(info.getUncompressedSize(), 24);

        try (OrcDataSource dataSource = fileOrcDataSource(newFile)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertEquals(reader.getTotalRowCount(), 2);
            assertEquals(reader.getFileRowCount(), 2);
            assertEquals(reader.getSplitLength(), newFile.length());

            assertEquals(reader.nextBatch(), 2);

            LongVector longVector = new LongVector(2);
            reader.readVector(0, longVector);
            for (int i = 0; i < 2; i++) {
                assertEquals(longVector.isNull[i], false);
            }
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
    public void testRewriteAllRowsDeleted()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);

        File file = new File(temporary, randomUUID().toString());
        try (OrcFileWriter writer = new OrcFileWriter(columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder(columnTypes).row(123).row(456).build());
        }

        BitSet rowsToDelete = new BitSet();
        rowsToDelete.set(0);
        rowsToDelete.set(1);

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = OrcFileRewriter.rewrite(file, newFile, rowsToDelete);
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
        try (OrcFileWriter writer = new OrcFileWriter(columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder(columnTypes).row(123).row(456).build());
        }

        BitSet rowsToDelete = new BitSet();

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = OrcFileRewriter.rewrite(file, newFile, rowsToDelete);
        assertEquals(info.getRowCount(), 2);
        assertEquals(info.getUncompressedSize(), 16);

        assertEquals(toByteArray(newFile), toByteArray(file));
    }

    @Test
    public void testUncompressedSize()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(1L, 2L, 3L, 4L, 5L);
        List<Type> columnTypes = ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, VARCHAR, VARBINARY);

        File file = new File(temporary, randomUUID().toString());
        try (OrcFileWriter writer = new OrcFileWriter(columnIds, columnTypes, file)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(true, 123, 98.7, "hello", utf8Slice("abc"))
                    .row(false, 456, 65.4, "world", utf8Slice("xyz"))
                    .row(null, null, null, null, null)
                    .build();
            writer.appendPages(pages);
        }

        File newFile = new File(temporary, randomUUID().toString());
        OrcFileInfo info = OrcFileRewriter.rewrite(file, newFile, new BitSet());
        assertEquals(info.getRowCount(), 3);
        assertEquals(info.getUncompressedSize(), 55);
    }
}
