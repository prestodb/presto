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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlTime;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.createFileWriter;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.fileOrcDataSource;
import static com.facebook.presto.raptor.storage.OrcTestingUtil.octets;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlocksEqual;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlocksEqual;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestShardWriter
{
    private File directory;

    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    @DataProvider(name = "useOptimizedOrcWriter")
    public static Object[][] useOptimizedOrcWriter()
    {
        return new Object[][] {{true}, {false}};
    }

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

    @Test(dataProvider = "useOptimizedOrcWriter")
    public void testWriter(boolean useOptimizedOrcWriter)
            throws Exception
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();

        List<Long> columnIds = ImmutableList.of(1L, 2L, 4L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L);
        ArrayType arrayType = new ArrayType(BIGINT);
        ArrayType arrayOfArrayType = new ArrayType(arrayType);
        Type mapType = functionAndTypeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(createVarcharType(10).getTypeSignature()),
                TypeSignatureParameter.of(BOOLEAN.getTypeSignature())));
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(10), VARBINARY, DOUBLE, BOOLEAN, arrayType, mapType, arrayOfArrayType, TIMESTAMP, TIME, DATE);
        File file = new File(directory, System.nanoTime() + ".orc");

        byte[] bytes1 = octets(0x00, 0xFE, 0xFF);
        byte[] bytes3 = octets(0x01, 0x02, 0x19, 0x80);

        long timestampValue = sqlTimestampOf(2002, 4, 6, 7, 8, 9, 0, UTC, UTC_KEY, SESSION).getMillisUtc();
        long timeValue = new SqlTime(NANOSECONDS.toMillis(new DateTime(2004, 11, 29, 0, 0, 0, 0, UTC).toLocalTime().getMillisOfDay())).getMillis();
        DateTime date = new DateTime(2001, 11, 22, 0, 0, 0, 0, UTC);
        int dateValue = new SqlDate(Days.daysBetween(new DateTime(0, ISOChronology.getInstanceUTC()), date).getDays()).getDays();
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(columnTypes)
                .row(123L, "hello", wrappedBuffer(bytes1), 123.456, true, arrayBlockOf(BIGINT, 1, 2), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)), timestampValue, timeValue, dateValue)
                .row(null, "world", null, Double.POSITIVE_INFINITY, null, arrayBlockOf(BIGINT, 3, null), mapBlockOf(createVarcharType(5), BOOLEAN, "k2", null), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 6, 7)), timestampValue, timeValue, dateValue)
                .row(456L, "bye \u2603", wrappedBuffer(bytes3), Double.NaN, false, arrayBlockOf(BIGINT), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", false), arrayBlockOf(arrayType, arrayBlockOf(BIGINT)), timestampValue, timeValue, dateValue);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(new EmptyClassLoader());
                FileWriter writer = createFileWriter(columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder.build());
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcBatchRecordReader reader = createReader(dataSource, columnIds, columnTypes);
            assertEquals(reader.getReaderRowCount(), 3);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFileRowCount(), reader.getReaderRowCount());
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            assertEquals(reader.nextBatch(), 3);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            Block column0 = reader.readBlock(0);
            assertEquals(column0.isNull(0), false);
            assertEquals(column0.isNull(1), true);
            assertEquals(column0.isNull(2), false);
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 2), 456L);

            Block column1 = reader.readBlock(1);
            assertEquals(createVarcharType(10).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(10).getSlice(column1, 1), utf8Slice("world"));
            assertEquals(createVarcharType(10).getSlice(column1, 2), utf8Slice("bye \u2603"));

            Block column2 = reader.readBlock(2);
            assertEquals(VARBINARY.getSlice(column2, 0), wrappedBuffer(bytes1));
            assertEquals(column2.isNull(1), true);
            assertEquals(VARBINARY.getSlice(column2, 2), wrappedBuffer(bytes3));

            Block column3 = reader.readBlock(3);
            assertEquals(column3.isNull(0), false);
            assertEquals(column3.isNull(1), false);
            assertEquals(column3.isNull(2), false);
            assertEquals(DOUBLE.getDouble(column3, 0), 123.456);
            assertEquals(DOUBLE.getDouble(column3, 1), Double.POSITIVE_INFINITY);
            assertEquals(DOUBLE.getDouble(column3, 2), Double.NaN);

            Block column4 = reader.readBlock(4);
            assertEquals(column4.isNull(0), false);
            assertEquals(column4.isNull(1), true);
            assertEquals(column4.isNull(2), false);
            assertEquals(BOOLEAN.getBoolean(column4, 0), true);
            assertEquals(BOOLEAN.getBoolean(column4, 2), false);

            Block column5 = reader.readBlock(5);
            assertEquals(column5.getPositionCount(), 3);

            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 0), arrayBlockOf(BIGINT, 1, 2)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 1), arrayBlockOf(BIGINT, 3, null)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 2), arrayBlockOf(BIGINT)));

            Block column6 = reader.readBlock(6);
            assertEquals(column6.getPositionCount(), 3);

            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column6, 0), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true)));
            Block object = arrayType.getObject(column6, 1);
            Block k2 = mapBlockOf(createVarcharType(5), BOOLEAN, "k2", null);
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, object, k2));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column6, 2), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", false)));

            Block column7 = reader.readBlock(7);
            assertEquals(column7.getPositionCount(), 3);

            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 1), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 6, 7))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 2), arrayBlockOf(arrayType, arrayBlockOf(BIGINT))));

            Block column8 = reader.readBlock(8);
            assertEquals(TIMESTAMP.getLong(column8, 0), timestampValue);
            assertEquals(TIMESTAMP.getLong(column8, 1), timestampValue);
            assertEquals(TIMESTAMP.getLong(column8, 2), timestampValue);

            Block column9 = reader.readBlock(9);
            assertEquals(TIME.getLong(column9, 0), timeValue);
            assertEquals(TIME.getLong(column9, 1), timeValue);
            assertEquals(TIME.getLong(column9, 2), timeValue);

            Block column10 = reader.readBlock(10);
            assertEquals(DATE.getLong(column10, 0), dateValue);
            assertEquals(DATE.getLong(column10, 1), dateValue);
            assertEquals(DATE.getLong(column10, 2), dateValue);

            assertEquals(reader.nextBatch(), -1);
            assertEquals(reader.getReaderPosition(), 3);
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            OrcFileMetadata orcFileMetadata = METADATA_CODEC.fromJson(reader.getUserMetadata().get(OrcFileMetadata.KEY).getBytes());
            assertEquals(orcFileMetadata, new OrcFileMetadata(ImmutableMap.<Long, TypeSignature>builder()
                    .put(1L, BIGINT.getTypeSignature())
                    .put(2L, createVarcharType(10).getTypeSignature())
                    .put(4L, VARBINARY.getTypeSignature())
                    .put(6L, DOUBLE.getTypeSignature())
                    .put(7L, BOOLEAN.getTypeSignature())
                    .put(8L, arrayType.getTypeSignature())
                    .put(9L, mapType.getTypeSignature())
                    .put(10L, arrayOfArrayType.getTypeSignature())
                    .put(11L, TIMESTAMP.getTypeSignature())
                    .put(12L, TIME.getTypeSignature())
                    .put(13L, DATE.getTypeSignature())
                    .build()));
        }

        File crcFile = new File(file.getParentFile(), "." + file.getName() + ".crc");
        assertFalse(crcFile.exists());

        // Test unsupported types
        for (Type type : ImmutableList.of(TIMESTAMP_WITH_TIME_ZONE, RowType.anonymous(ImmutableList.of(BIGINT, DOUBLE)))) {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(new EmptyClassLoader())) {
                createFileWriter(ImmutableList.of(1L), ImmutableList.of(type), file);
                fail();
            }
            catch (PrestoException e) {
                assertTrue(e.getMessage().toLowerCase(ENGLISH).contains("type"));
            }
        }
    }

    @SuppressWarnings("EmptyTryBlock")
    @Test
    public void testWriterZeroRows()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(1L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);

        File file = new File(directory, System.nanoTime() + ".orc");

        // optimized ORC writer will flush metadata on close
        try (FileWriter ignored = createFileWriter(columnIds, columnTypes, file)) {
            // no rows
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcBatchRecordReader reader = createReader(dataSource, columnIds, columnTypes);
            assertEquals(reader.getReaderRowCount(), 0);
            assertEquals(reader.getReaderPosition(), 0);

            assertEquals(reader.nextBatch(), -1);
        }
    }

    @SuppressWarnings("EmptyClass")
    private static class EmptyClassLoader
            extends ClassLoader
    {
        protected EmptyClassLoader()
        {
            super(null);
        }
    }
}
