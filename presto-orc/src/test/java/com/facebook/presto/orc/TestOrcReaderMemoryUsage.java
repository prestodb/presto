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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.createCustomOrcRecordReader;
import static com.facebook.presto.orc.OrcTester.createCustomOrcSelectiveRecordReader;
import static com.facebook.presto.orc.OrcTester.createOrcRecordWriter;
import static com.facebook.presto.orc.OrcTester.createSettableStructObjectInspector;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;
import static com.facebook.presto.util.Reflection.methodHandle;
import static org.testng.Assert.assertEquals;

public class TestOrcReaderMemoryUsage
{
    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testMainTableFlatMapMemoryFootPrint()
            throws Exception
    {
        MethodHandle methodHandle = methodHandle(TestOrcReaderMemoryUsage.class, "throwUnsupportedOperation");
        //list of selected input columns
        Map<Integer, Type> inputColumns = ImmutableMap.of(
                6, new MapType(INTEGER, REAL, methodHandle, methodHandle),
                7, new MapType(INTEGER, new ArrayType(BIGINT), methodHandle, methodHandle),
                11, new MapType(INTEGER, new MapType(BIGINT, REAL, methodHandle, methodHandle), methodHandle, methodHandle));
        // schema of side table
        List<Type> allTypes = ImmutableList.of(
                BIGINT,
                BIGINT,
                BIGINT,
                DOUBLE,
                DOUBLE,
                DOUBLE,
                new MapType(INTEGER, REAL, methodHandle, methodHandle), // float_features
                new MapType(INTEGER, new ArrayType(BIGINT), methodHandle, methodHandle), // id_list_features
                TINYINT,
                new MapType(INTEGER, BIGINT, methodHandle, methodHandle), // join_keys
                new MapType(INTEGER, DOUBLE, methodHandle, methodHandle), // multi_labels
                new MapType(INTEGER, new MapType(BIGINT, REAL, methodHandle, methodHandle), methodHandle, methodHandle), // id_score_list_features
                VARCHAR,
                BIGINT,
                new MapType(INTEGER, new ArrayType(TINYINT), methodHandle, methodHandle), // native_bytes_array_features
                new MapType(INTEGER, new ArrayType(new ArrayType(BIGINT)), methodHandle, methodHandle)); // event_based_features;

        // list of output columns
        List<Integer> outputColumns = ImmutableList.of(6, 7, 11);
        testFlatMapMemoryFootPrint("/Users/hrastogi/.main_table_29", inputColumns, allTypes, outputColumns);
    }

    @Test
    public void testSideTableFlatMapMemoryFootPrint()
            throws Exception
    {
        MethodHandle methodHandle = methodHandle(TestOrcReaderMemoryUsage.class, "throwUnsupportedOperation");
        //list of selected input columns
        Map<Integer, Type> inputColumns = ImmutableMap.of(
                1, new MapType(INTEGER, REAL, methodHandle, methodHandle),
                2, new MapType(INTEGER, new ArrayType(BIGINT), methodHandle, methodHandle),
                3, new MapType(INTEGER, new MapType(BIGINT, REAL, methodHandle, methodHandle), methodHandle, methodHandle));
        // schema of side table
        List<Type> allTypes = ImmutableList.of(
                new MapType(VARCHAR, BIGINT, methodHandle, methodHandle),
                new MapType(INTEGER, REAL, methodHandle, methodHandle),
                new MapType(INTEGER, new ArrayType(BIGINT), methodHandle, methodHandle),
                new MapType(INTEGER, new MapType(BIGINT, REAL, methodHandle, methodHandle), methodHandle, methodHandle),
                new MapType(VARCHAR, VARCHAR, methodHandle, methodHandle));
        // list of output columns
        List<Integer> outputColumns = ImmutableList.of(1, 2, 3);
        testFlatMapMemoryFootPrint("/Users/hrastogi/.side_table_bugger", inputColumns, allTypes, outputColumns);
    }

    public void testFlatMapMemoryFootPrint(String filePath, Map<Integer, Type> inputColumns, List<Type> allTypes, List<Integer> outputColumns) throws Exception
    {
        OrcSelectiveRecordReader reader;
        File localFile = new File(filePath);
        reader = createCustomOrcSelectiveRecordReader(
                localFile,
                DWRF.getOrcEncoding(),
                OrcPredicate.TRUE,
                allTypes,
                MAX_BATCH_SIZE,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                inputColumns,
                outputColumns,
                true,
                new TestingHiveOrcAggregatedMemoryContext(),
                false);
        long retainedMemoryInSize = 0;
        Page nextPage;
        while ((nextPage = reader.getNextPage()) != null) {
            for (int i = 0; i < nextPage.getChannelCount(); i++) {
                nextPage.getLoadedPage();
            }
            retainedMemoryInSize = Math.max(reader.getRetainedSizeInBytes(), retainedMemoryInSize);
        }
        // print the output map column
        System.out.println("Max retainedMemoryInSize is " + retainedMemoryInSize);
    }

    @Test
    public void testVarcharTypeWithoutNulls()
            throws Exception
    {
        int rows = 5000;
        OrcBatchRecordReader reader = null;
        try (TempFile tempFile = createSingleColumnVarcharFile(rows, 10)) {
            reader = createCustomOrcRecordReader(tempFile, ORC, OrcPredicate.TRUE, VARCHAR, INITIAL_BATCH_SIZE, false, false);
            assertInitialRetainedSizes(reader, rows);

            long stripeReaderRetainedSize = reader.getCurrentStripeRetainedSizeInBytes();
            long streamReaderRetainedSize = reader.getStreamReaderRetainedSizeInBytes();
            long readerRetainedSize = reader.getRetainedSizeInBytes();
            long readerSystemMemoryUsage = reader.getSystemMemoryUsage();

            while (true) {
                int batchSize = reader.nextBatch();
                if (batchSize == -1) {
                    break;
                }

                Block block = reader.readBlock(0);
                assertEquals(block.getPositionCount(), batchSize);

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (batchSize < MAX_BATCH_SIZE) {
                    continue;
                }

                // StripeReader memory should increase after reading a block.
                assertGreaterThan(reader.getCurrentStripeRetainedSizeInBytes(), stripeReaderRetainedSize);
                // SliceDictionaryBatchStreamReader uses stripeDictionaryLength local buffer.
                assertEquals(reader.getStreamReaderRetainedSizeInBytes() - streamReaderRetainedSize, 49L);
                // The total retained size and system memory usage should be greater than 0 byte because of the instance sizes.
                assertGreaterThan(reader.getRetainedSizeInBytes() - readerRetainedSize, 0L);
                assertGreaterThan(reader.getSystemMemoryUsage() - readerSystemMemoryUsage, 0L);
            }
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
        assertClosedRetainedSizes(reader);
    }

    @Test
    public void testBigIntTypeWithNulls()
            throws Exception
    {
        int rows = 10000;
        OrcBatchRecordReader reader = null;
        try (TempFile tempFile = createSingleColumnFileWithNullValues(rows)) {
            reader = createCustomOrcRecordReader(tempFile, ORC, OrcPredicate.TRUE, BIGINT, INITIAL_BATCH_SIZE, false, false);
            assertInitialRetainedSizes(reader, rows);

            long stripeReaderRetainedSize = reader.getCurrentStripeRetainedSizeInBytes();
            long streamReaderRetainedSize = reader.getStreamReaderRetainedSizeInBytes();
            long readerRetainedSize = reader.getRetainedSizeInBytes();
            long readerSystemMemoryUsage = reader.getSystemMemoryUsage();

            while (true) {
                int batchSize = reader.nextBatch();
                if (batchSize == -1) {
                    break;
                }

                Block block = reader.readBlock(0);
                assertEquals(block.getPositionCount(), batchSize);

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (batchSize < MAX_BATCH_SIZE) {
                    continue;
                }

                // StripeReader memory should increase after reading a block.
                assertGreaterThan(reader.getCurrentStripeRetainedSizeInBytes(), stripeReaderRetainedSize);
                // There are no local buffers needed.
                assertEquals(reader.getStreamReaderRetainedSizeInBytes() - streamReaderRetainedSize, 0L);
                // The total retained size and system memory usage should be strictly larger than 0L because of the instance sizes.
                assertGreaterThan(reader.getRetainedSizeInBytes() - readerRetainedSize, 0L);
                assertGreaterThan(reader.getSystemMemoryUsage() - readerSystemMemoryUsage, 0L);
            }
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
        assertClosedRetainedSizes(reader);
    }

    @Test
    public void testMapTypeWithNulls()
            throws Exception
    {
        Type keyType = BIGINT;
        Type valueType = BIGINT;

        MethodHandle keyNativeEquals = getOperatorMethodHandle(OperatorType.EQUAL, keyType, keyType);
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = getOperatorMethodHandle(OperatorType.HASH_CODE, keyType);
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));

        MapType mapType = new MapType(
                keyType,
                valueType,
                keyBlockEquals,
                keyBlockHashCode);

        int rows = 10000;
        OrcBatchRecordReader reader = null;
        try (TempFile tempFile = createSingleColumnMapFileWithNullValues(mapType, rows)) {
            reader = createCustomOrcRecordReader(tempFile, ORC, OrcPredicate.TRUE, mapType, INITIAL_BATCH_SIZE, false, false);
            assertInitialRetainedSizes(reader, rows);

            long stripeReaderRetainedSize = reader.getCurrentStripeRetainedSizeInBytes();
            long streamReaderRetainedSize = reader.getStreamReaderRetainedSizeInBytes();
            long readerRetainedSize = reader.getRetainedSizeInBytes();
            long readerSystemMemoryUsage = reader.getSystemMemoryUsage();

            while (true) {
                int batchSize = reader.nextBatch();
                if (batchSize == -1) {
                    break;
                }

                Block block = reader.readBlock(0);
                assertEquals(block.getPositionCount(), batchSize);

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (batchSize < MAX_BATCH_SIZE) {
                    continue;
                }

                // StripeReader memory should increase after reading a block.
                assertGreaterThan(reader.getCurrentStripeRetainedSizeInBytes(), stripeReaderRetainedSize);
                // There are no local buffers needed.
                assertEquals(reader.getStreamReaderRetainedSizeInBytes() - streamReaderRetainedSize, 0L);
                // The total retained size and system memory usage should be strictly larger than 0L because of the instance sizes.
                assertGreaterThan(reader.getRetainedSizeInBytes() - readerRetainedSize, 0L);
                assertGreaterThan(reader.getSystemMemoryUsage() - readerSystemMemoryUsage, 0L);
            }
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
        assertClosedRetainedSizes(reader);
    }

    /**
     * Write a file that contains a number of rows with 1 BIGINT column, and some rows have null values.
     */
    private static TempFile createSingleColumnFileWithNullValues(int rows)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        Serializer serde = new OrcSerde();
        TempFile tempFile = new TempFile();
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(tempFile.getFile(), ORC_12, CompressionKind.NONE, BIGINT);
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", BIGINT);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < rows; i++) {
            if (i % 10 == 0) {
                objectInspector.setStructFieldData(row, field, null);
            }
            else {
                objectInspector.setStructFieldData(row, field, (long) i);
            }

            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
        return tempFile;
    }

    /**
     * Write a file that contains a number of rows with 1 VARCHAR column, and all values are not null.
     */
    private static TempFile createSingleColumnVarcharFile(int count, int length)
            throws Exception
    {
        Serializer serde = new OrcSerde();
        TempFile tempFile = new TempFile();
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(tempFile.getFile(), ORC_12, CompressionKind.NONE, VARCHAR);
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", VARCHAR);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < count; i++) {
            objectInspector.setStructFieldData(row, field, Strings.repeat("0", length));
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
        return tempFile;
    }

    /**
     * Write a file that contains a given number of maps where each row has 10 entries in total
     * and some entries have null keys/values.
     */
    private static TempFile createSingleColumnMapFileWithNullValues(Type mapType, int rows)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        Serializer serde = new OrcSerde();
        TempFile tempFile = new TempFile();
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(tempFile.getFile(), ORC_12, CompressionKind.NONE, mapType);
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", mapType);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 1; i <= rows; i++) {
            HashMap<Long, Long> map = new HashMap<>();

            for (int j = 1; j <= 8; j++) {
                Long value = (long) j;
                map.put(value, value);
            }

            // Add null values so that the StreamReader nullVectors are not empty.
            map.put(null, 0L);
            map.put(0L, null);

            objectInspector.setStructFieldData(row, field, map);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }
        writer.close(false);
        return tempFile;
    }

    private static void assertInitialRetainedSizes(OrcBatchRecordReader reader, int rows)
    {
        assertEquals(reader.getReaderRowCount(), rows);
        assertEquals(reader.getReaderPosition(), 0);
        assertEquals(reader.getCurrentStripeRetainedSizeInBytes(), 0);
        // there will be object overheads
        assertGreaterThan(reader.getStreamReaderRetainedSizeInBytes(), 0L);
        // there will be object overheads
        assertGreaterThan(reader.getRetainedSizeInBytes(), 0L);
        assertEquals(reader.getSystemMemoryUsage(), 0);
    }

    private static void assertClosedRetainedSizes(OrcBatchRecordReader reader)
    {
        assertEquals(reader.getCurrentStripeRetainedSizeInBytes(), 0);
        // after close() we still account for the StreamReader instance sizes.
        assertGreaterThan(reader.getStreamReaderRetainedSizeInBytes(), 0L);
        // after close() we still account for the StreamReader instance sizes.
        assertGreaterThan(reader.getRetainedSizeInBytes(), 0L);
        assertEquals(reader.getSystemMemoryUsage(), 0);
    }
}
