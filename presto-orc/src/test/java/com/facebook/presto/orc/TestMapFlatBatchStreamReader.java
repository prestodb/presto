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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.TestMapFlatBatchStreamReader.ExpectedValuesBuilder.Frequency.ALL;
import static com.facebook.presto.orc.TestMapFlatBatchStreamReader.ExpectedValuesBuilder.Frequency.ALL_EXCEPT_FIRST;
import static com.facebook.presto.orc.TestMapFlatBatchStreamReader.ExpectedValuesBuilder.Frequency.NONE;
import static com.facebook.presto.orc.TestMapFlatBatchStreamReader.ExpectedValuesBuilder.Frequency.SOME;
import static com.facebook.presto.orc.TestingOrcPredicate.createOrcPredicate;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterators.advance;
import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestMapFlatBatchStreamReader
{
    // TODO: Add tests for timestamp as value type

    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();
    private static final int NUM_ROWS = 31_234;

    private static final Type LIST_TYPE = FUNCTION_AND_TYPE_MANAGER.getParameterizedType(
            StandardTypes.ARRAY,
            ImmutableList.of(TypeSignatureParameter.of(IntegerType.INTEGER.getTypeSignature())));
    private static final Type MAP_TYPE = FUNCTION_AND_TYPE_MANAGER.getParameterizedType(
            StandardTypes.MAP,
            ImmutableList.of(TypeSignatureParameter.of(VarcharType.VARCHAR.getTypeSignature()), TypeSignatureParameter.of(RealType.REAL.getTypeSignature())));
    private static final Type STRUCT_TYPE = FUNCTION_AND_TYPE_MANAGER.getParameterizedType(
            StandardTypes.ROW,
            ImmutableList.of(
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value1", false)), IntegerType.INTEGER.getTypeSignature())),
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value2", false)), IntegerType.INTEGER.getTypeSignature())),
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value3", false)), IntegerType.INTEGER.getTypeSignature()))));

    @Test
    public void testByte()
            throws Exception
    {
        runTest("test_flat_map/flat_map_byte.dwrf",
                TinyintType.TINYINT,
                ExpectedValuesBuilder.get(Integer::byteValue));
    }

    @Test
    public void testByteWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_byte_with_null.dwrf",
                TinyintType.TINYINT,
                ExpectedValuesBuilder.get(Integer::byteValue).setNullValuesFrequency(SOME));
    }

    @Test
    public void testShort()
            throws Exception
    {
        runTest("test_flat_map/flat_map_short.dwrf",
                SmallintType.SMALLINT,
                ExpectedValuesBuilder.get(Integer::shortValue));
    }

    @Test
    public void testInteger()
            throws Exception
    {
        runTest("test_flat_map/flat_map_int.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()));
    }

    @Test
    public void testIntegerWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_int_with_null.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullValuesFrequency(SOME));
    }

    @Test
    public void testIntegerWithSharedDictionary()
            throws Exception
    {
        runTest("test_flat_map/flat_map_dict_share_simple.dwrf",
                INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNumRows(2048));
    }

    @Test
    public void testLong()
            throws Exception
    {
        runTest("test_flat_map/flat_map_long.dwrf",
                BigintType.BIGINT,
                ExpectedValuesBuilder.get(Integer::longValue));
    }

    @Test
    public void testString()
            throws Exception
    {
        runTest("test_flat_map/flat_map_string.dwrf",
                VarcharType.VARCHAR,
                ExpectedValuesBuilder.get(i -> Integer.toString(i)));
    }

    @Test
    public void testStringWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_string_with_null.dwrf",
                VarcharType.VARCHAR,
                ExpectedValuesBuilder.get(i -> Integer.toString(i)).setNullValuesFrequency(SOME));
    }

    @Test
    public void testBinary()
            throws Exception
    {
        runTest("test_flat_map/flat_map_binary.dwrf",
                VarbinaryType.VARBINARY,
                ExpectedValuesBuilder.get(i -> new SqlVarbinary(Integer.toString(i).getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        runTest("test_flat_map/flat_map_boolean.dwrf",
                IntegerType.INTEGER,
                BooleanType.BOOLEAN,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatBatchStreamReader::intToBoolean));
    }

    @Test
    public void testBooleanWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_boolean_with_null.dwrf",
                IntegerType.INTEGER,
                BooleanType.BOOLEAN,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatBatchStreamReader::intToBoolean).setNullValuesFrequency(SOME));
    }

    @Test
    public void testFloat()
            throws Exception
    {
        runTest("test_flat_map/flat_map_float.dwrf",
                IntegerType.INTEGER,
                RealType.REAL,
                ExpectedValuesBuilder.get(Function.identity(), Float::valueOf));
    }

    @Test
    public void testFloatWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_float_with_null.dwrf",
                IntegerType.INTEGER,
                RealType.REAL,
                ExpectedValuesBuilder.get(Function.identity(), Float::valueOf).setNullValuesFrequency(SOME));
    }

    @Test
    public void testDouble()
            throws Exception
    {
        runTest("test_flat_map/flat_map_double.dwrf",
                IntegerType.INTEGER,
                DoubleType.DOUBLE,
                ExpectedValuesBuilder.get(Function.identity(), Double::valueOf));
    }

    @Test
    public void testDoubleWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_double_with_null.dwrf",
                IntegerType.INTEGER,
                DoubleType.DOUBLE,
                ExpectedValuesBuilder.get(Function.identity(), Double::valueOf).setNullValuesFrequency(SOME));
    }

    @Test
    public void testList()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_list.dwrf",
                IntegerType.INTEGER,
                LIST_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatBatchStreamReader::intToList));
    }

    @Test
    public void testListWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_list_with_null.dwrf",
                IntegerType.INTEGER,
                LIST_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatBatchStreamReader::intToList).setNullValuesFrequency(SOME));
    }

    @Test
    public void testMap()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_map.dwrf",
                IntegerType.INTEGER,
                MAP_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatBatchStreamReader::intToMap));
    }

    @Test
    public void testMapWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_map_with_null.dwrf",
                IntegerType.INTEGER,
                MAP_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatBatchStreamReader::intToMap).setNullValuesFrequency(SOME));
    }

    @Test
    public void testMapWithSharedDictionary()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_dict_share_nested.dwrf",
                BIGINT,
                mapType(VARCHAR, INTEGER),
                ExpectedValuesBuilder.get(x -> (long) x, TestMapFlatBatchStreamReader::intToIntMap).setNumRows(2048));
    }

    @Test
    public void testStruct()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_struct.dwrf",
                IntegerType.INTEGER,
                STRUCT_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatBatchStreamReader::intToList));
    }

    @Test
    public void testStructWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_struct_with_null.dwrf",
                IntegerType.INTEGER,
                STRUCT_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatBatchStreamReader::intToList).setNullValuesFrequency(SOME));
    }

    @Test
    public void testWithNulls()
            throws Exception
    {
        // A test case where some of the flat maps are null
        runTest(
                "test_flat_map/flat_map_some_null_maps.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullRowsFrequency(SOME));
    }

    @Test
    public void testWithAllNulls()
            throws Exception
    {
        // A test case where every flat map is null
        runTest(
                "test_flat_map/flat_map_all_null_maps.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullRowsFrequency(ALL));
    }

    @Test
    public void testWithAllNullsExceptFirst()
            throws Exception
    {
        // A test case where every flat map is null except the first one
        runTest(
                "test_flat_map/flat_map_all_null_maps_except_first.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullRowsFrequency(ALL_EXCEPT_FIRST));
    }

    @Test
    public void testWithEmptyMaps()
            throws Exception
    {
        // A test case where some of the flat maps are empty
        runTest(
                "test_flat_map/flat_map_some_empty_maps.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setEmptyMapsFrequency(SOME));
    }

    @Test
    public void testWithAllMaps()
            throws Exception
    {
        // A test case where all of the flat maps are empty
        runTest(
                "test_flat_map/flat_map_all_empty_maps.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setEmptyMapsFrequency(ALL));
    }

    // All maps are empty and encoding is not present
    @Test
    public void testWithAllEmptyMapsWithNoEncoding()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_all_empty_maps_no_encoding.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setEmptyMapsFrequency(ALL));
    }

    @Test
    public void testMixedEncodings()
            throws Exception
    {
        // A test case where the values associated with one key are direct encoded, and all other keys are
        // dictionary encoded.  The dictionary encoded values have occasional values that only appear once
        // to ensure the IN_DICTIONARY stream is present, which means the checkpoints for dictionary encoded
        // values will have a different number of positions compared to direct encoded values.
        runTest("test_flat_map/flat_map_mixed_encodings.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setMixedEncodings());
    }

    @Test
    public void testIntegerWithMissingSequences()
            throws Exception
    {
        // A test case where the additional sequences IDs for a flat map aren't a consecutive range [1,N], the odd
        // sequence IDs have been removed.  This is to simulate the case where a file has been modified to delete
        // certain keys from the map by dropping the ColumnEncodings and the associated data.
        runTest("test_flat_map/flat_map_int_missing_sequences.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setMissingSequences());
    }

    @Test
    public void testIntegerWithMissingSequence0()
            throws Exception
    {
        // A test case where the (dummy) encoding for sequence 0 of the value node doesn't exist
        runTest("test_flat_map/flat_map_int_missing_sequence_0.dwrf",
                IntegerType.INTEGER,
                ExpectedValuesBuilder.get(Function.identity()));
    }

    private <K, V> void runTest(String testOrcFileName, Type type, ExpectedValuesBuilder<K, V> expectedValuesBuilder)
            throws Exception
    {
        runTest(testOrcFileName, type, type, expectedValuesBuilder);
    }

    private <K, V> void runTest(String testOrcFileName, Type keyType, Type valueType, ExpectedValuesBuilder<K, V> expectedValuesBuilder)
            throws Exception
    {
        List<Map<K, V>> expectedValues = expectedValuesBuilder.build();

        runTest(testOrcFileName, keyType, valueType, expectedValues, false, false);
        runTest(testOrcFileName, keyType, valueType, expectedValues, true, false);
        runTest(testOrcFileName, keyType, valueType, expectedValues, false, true);
    }

    private <K, V> void runTest(String testOrcFileName, Type keyType, Type valueType, List<Map<K, V>> expectedValues, boolean skipFirstBatch, boolean skipFirstStripe)
            throws Exception
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(
                new File(getResource(testOrcFileName).getFile()),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);
        OrcReader orcReader = new OrcReader(
                orcDataSource,
                OrcEncoding.DWRF,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                OrcReaderTestingUtils.createDefaultTestConfig(),
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats());
        Type mapType = FUNCTION_AND_TYPE_MANAGER.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(
                        TypeSignatureParameter.of(keyType.getTypeSignature()),
                        TypeSignatureParameter.of(valueType.getTypeSignature())));

        try (OrcBatchRecordReader recordReader = orcReader.createBatchRecordReader(
                ImmutableMap.of(0, mapType),
                createOrcPredicate(0, mapType, expectedValues, OrcTester.Format.DWRF, true),
                HIVE_STORAGE_TIME_ZONE,
                new TestingHiveOrcAggregatedMemoryContext(),
                1024)) {
            Iterator<?> expectedValuesIterator = expectedValues.iterator();

            boolean isFirst = true;
            int rowsProcessed = 0;
            for (int batchSize = toIntExact(recordReader.nextBatch()); batchSize >= 0; batchSize = toIntExact(recordReader.nextBatch())) {
                if (skipFirstStripe && rowsProcessed < 10_000) {
                    assertEquals(advance(expectedValuesIterator, batchSize), batchSize);
                }
                else if (skipFirstBatch && isFirst) {
                    assertEquals(advance(expectedValuesIterator, batchSize), batchSize);
                    isFirst = false;
                }
                else {
                    Block block = recordReader.readBlock(0);

                    for (int position = 0; position < block.getPositionCount(); position++) {
                        assertEquals(mapType.getObjectValue(SESSION.getSqlFunctionProperties(), block, position), expectedValuesIterator.next(), String.format("row mismatch at processed rows %d, position %d", rowsProcessed, position));
                    }
                }

                assertEquals(recordReader.getReaderPosition(), rowsProcessed);
                assertEquals(recordReader.getFilePosition(), rowsProcessed);
                rowsProcessed += batchSize;
            }

            assertFalse(expectedValuesIterator.hasNext());
            assertEquals(recordReader.getReaderPosition(), rowsProcessed);
            assertEquals(recordReader.getFilePosition(), rowsProcessed);
        }
    }

    private static boolean intToBoolean(int i)
    {
        return i % 2 == 0;
    }

    private static SqlTimestamp intToTimestamp(int i)
    {
        return new SqlTimestamp(i, TimeZoneKey.UTC_KEY);
    }

    private static List<Integer> intToList(int i)
    {
        return ImmutableList.of(i * 3, i * 3 + 1, i * 3 + 2);
    }

    private static Map<String, Float> intToMap(int i)
    {
        return ImmutableMap.of(Integer.toString(i * 3), (float) (i * 3), Integer.toString(i * 3 + 1), (float) (i * 3 + 1), Integer.toString(i * 3 + 2), (float) (i * 3 + 2));
    }

    private static Map<String, Integer> intToIntMap(int i)
    {
        return ImmutableMap.of(Integer.toString(i * 3), i * 3, Integer.toString(i * 3 + 1), i * 3 + 1, Integer.toString(i * 3 + 2), i * 3 + 2);
    }

    static class ExpectedValuesBuilder<K, V>
    {
        enum Frequency
        {
            NONE,
            SOME,
            ALL,
            ALL_EXCEPT_FIRST,
        }

        private final Function<Integer, K> keyConverter;
        private final Function<Integer, V> valueConverter;
        private Frequency nullValuesFrequency = NONE;
        private Frequency nullRowsFrequency = NONE;
        private Frequency emptyMapsFrequency = NONE;
        private boolean mixedEncodings;
        private boolean missingSequences;
        private int numRows = NUM_ROWS;

        private ExpectedValuesBuilder(Function<Integer, K> keyConverter, Function<Integer, V> valueConverter)
        {
            this.keyConverter = keyConverter;
            this.valueConverter = valueConverter;
        }

        public static <T> ExpectedValuesBuilder<T, T> get(Function<Integer, T> converter)
        {
            return new ExpectedValuesBuilder<>(converter, converter);
        }

        public static <K, V> ExpectedValuesBuilder<K, V> get(Function<Integer, K> keyConverter, Function<Integer, V> valueConverter)
        {
            return new ExpectedValuesBuilder<>(keyConverter, valueConverter);
        }

        public ExpectedValuesBuilder<K, V> setNullValuesFrequency(Frequency frequency)
        {
            this.nullValuesFrequency = frequency;

            return this;
        }

        public ExpectedValuesBuilder<K, V> setNullRowsFrequency(Frequency frequency)
        {
            this.nullRowsFrequency = frequency;

            return this;
        }

        public ExpectedValuesBuilder<K, V> setEmptyMapsFrequency(Frequency frequency)
        {
            this.emptyMapsFrequency = frequency;

            return this;
        }

        public ExpectedValuesBuilder<K, V> setMixedEncodings()
        {
            this.mixedEncodings = true;

            return this;
        }

        public ExpectedValuesBuilder<K, V> setMissingSequences()
        {
            this.missingSequences = true;

            return this;
        }

        public ExpectedValuesBuilder<K, V> setNumRows(int numRows)
        {
            this.numRows = numRows;
            return this;
        }

        public List<Map<K, V>> build()
        {
            List<Map<K, V>> result = new ArrayList<>(numRows);

            for (int i = 0; i < numRows; ++i) {
                if (passesFrequencyCheck(nullRowsFrequency, i)) {
                    result.add(null);
                }
                else if (passesFrequencyCheck(emptyMapsFrequency, i)) {
                    result.add(Collections.emptyMap());
                }
                else {
                    Map<K, V> row = new HashMap<>();

                    for (int j = 0; j < 3; j++) {
                        V value;
                        int key = (i * 3 + j) % 32;

                        if (missingSequences && key % 2 == 1) {
                            continue;
                        }

                        if (j == 0 && passesFrequencyCheck(nullValuesFrequency, i)) {
                            value = null;
                        }
                        else if (mixedEncodings && (key == 1 || j == 2)) {
                            // TODO: add comments to explain the condition
                            value = valueConverter.apply(i * 3 + j);
                        }
                        else {
                            value = valueConverter.apply((i * 3 + j) % 32);
                        }

                        row.put(keyConverter.apply(key), value);
                    }

                    result.add(row);
                }
            }

            return result;
        }

        private boolean passesFrequencyCheck(Frequency frequency, int i)
        {
            switch (frequency) {
                case NONE:
                    return false;
                case ALL:
                    return true;
                case SOME:
                    return i % 5 == 0;
                case ALL_EXCEPT_FIRST:
                    return i != 0;
                default:
                    throw new IllegalArgumentException("Got unexpected Frequency: " + frequency);
            }
        }
    }
}
