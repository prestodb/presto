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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.relation.Predicate;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcTester.arrayType;
import static com.facebook.presto.orc.OrcTester.assertFileContentsPresto;
import static com.facebook.presto.orc.OrcTester.filterRows;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.TestMapFlatSelectiveStreamReader.ExpectedValuesBuilder.Frequency.ALL;
import static com.facebook.presto.orc.TestMapFlatSelectiveStreamReader.ExpectedValuesBuilder.Frequency.ALL_EXCEPT_FIRST;
import static com.facebook.presto.orc.TestMapFlatSelectiveStreamReader.ExpectedValuesBuilder.Frequency.NONE;
import static com.facebook.presto.orc.TestMapFlatSelectiveStreamReader.ExpectedValuesBuilder.Frequency.SOME;
import static com.facebook.presto.orc.TestingOrcPredicate.createOrcPredicate;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.TupleDomainFilterUtils.toBigintValues;
import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static java.util.stream.Collectors.toList;

public class TestMapFlatSelectiveStreamReader
{
    // TODO: Add tests for timestamp as value type

    private static final Type STRUCT_TYPE = FUNCTION_AND_TYPE_MANAGER.getParameterizedType(
            StandardTypes.ROW,
            ImmutableList.of(
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value1", false)), IntegerType.INTEGER.getTypeSignature())),
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value2", false)), IntegerType.INTEGER.getTypeSignature())),
                    TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("value3", false)), IntegerType.INTEGER.getTypeSignature()))));

    private static final int NUM_ROWS = 31_234;

    @Test
    public void testByte()
            throws Exception
    {
        runTest("test_flat_map/flat_map_byte.dwrf",
                TINYINT,
                ExpectedValuesBuilder.get(Integer::byteValue));
    }

    @Test
    public void testByteWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_byte_with_null.dwrf",
                TINYINT,
                ExpectedValuesBuilder.get(Integer::byteValue).setNullValuesFrequency(SOME));
    }

    @Test
    public void testShort()
            throws Exception
    {
        runTest("test_flat_map/flat_map_short.dwrf",
                SMALLINT,
                ExpectedValuesBuilder.get(Integer::shortValue));
    }

    @Test
    public void testInteger()
            throws Exception
    {
        runTest("test_flat_map/flat_map_int.dwrf",
                INTEGER,
                ExpectedValuesBuilder.get(Function.identity()));
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
    public void testIntegerWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_int_with_null.dwrf",
                INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullValuesFrequency(SOME));
    }

    @Test
    public void testLong()
            throws Exception
    {
        runTest("test_flat_map/flat_map_long.dwrf",
                BIGINT,
                ExpectedValuesBuilder.get(Integer::longValue));
    }

    @Test
    public void testString()
            throws Exception
    {
        runTest("test_flat_map/flat_map_string.dwrf",
                VARCHAR,
                ExpectedValuesBuilder.get(i -> Integer.toString(i)));
    }

    @Test
    public void testStringWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_string_with_null.dwrf",
                VARCHAR,
                ExpectedValuesBuilder.get(i -> Integer.toString(i)).setNullValuesFrequency(SOME));
    }

    @Test
    public void testBinary()
            throws Exception
    {
        runTest("test_flat_map/flat_map_binary.dwrf",
                VARBINARY,
                ExpectedValuesBuilder.get(i -> new SqlVarbinary(Integer.toString(i).getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        runTest("test_flat_map/flat_map_boolean.dwrf",
                INTEGER,
                BooleanType.BOOLEAN,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatSelectiveStreamReader::intToBoolean));
    }

    @Test
    public void testBooleanWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_boolean_with_null.dwrf",
                INTEGER,
                BooleanType.BOOLEAN,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatSelectiveStreamReader::intToBoolean).setNullValuesFrequency(SOME));
    }

    @Test
    public void testFloat()
            throws Exception
    {
        runTest("test_flat_map/flat_map_float.dwrf",
                INTEGER,
                REAL,
                ExpectedValuesBuilder.get(Function.identity(), Float::valueOf));
    }

    @Test
    public void testFloatWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_float_with_null.dwrf",
                INTEGER,
                REAL,
                ExpectedValuesBuilder.get(Function.identity(), Float::valueOf).setNullValuesFrequency(SOME));
    }

    @Test
    public void testDouble()
            throws Exception
    {
        runTest("test_flat_map/flat_map_double.dwrf",
                INTEGER,
                DoubleType.DOUBLE,
                ExpectedValuesBuilder.get(Function.identity(), Double::valueOf));
    }

    @Test
    public void testDoubleWithNull()
            throws Exception
    {
        runTest("test_flat_map/flat_map_double_with_null.dwrf",
                INTEGER,
                DoubleType.DOUBLE,
                ExpectedValuesBuilder.get(Function.identity(), Double::valueOf).setNullValuesFrequency(SOME));
    }

    @Test
    public void testList()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_list.dwrf",
                INTEGER,
                arrayType(INTEGER),
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatSelectiveStreamReader::intToList));
    }

    @Test
    public void testListWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_list_with_null.dwrf",
                INTEGER,
                arrayType(INTEGER),
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatSelectiveStreamReader::intToList).setNullValuesFrequency(SOME));
    }

    @Test
    public void testMap()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_map.dwrf",
                INTEGER,
                mapType(VARCHAR, REAL),
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatSelectiveStreamReader::intToMap));
    }

    @Test
    public void testMapWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_map_with_null.dwrf",
                INTEGER,
                mapType(VARCHAR, REAL),
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatSelectiveStreamReader::intToMap).setNullValuesFrequency(SOME));
    }

    @Test
    public void testMapWithSharedDictionary()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_dict_share_nested.dwrf",
                BIGINT,
                mapType(VARCHAR, INTEGER),
                ExpectedValuesBuilder.get(x -> (long) x, TestMapFlatSelectiveStreamReader::intToIntMap).setNumRows(2048));
    }

    @Test
    public void testStruct()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_struct.dwrf",
                INTEGER,
                STRUCT_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatSelectiveStreamReader::intToList));
    }

    @Test
    public void testStructWithNull()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_struct_with_null.dwrf",
                INTEGER,
                STRUCT_TYPE,
                ExpectedValuesBuilder.get(Function.identity(), TestMapFlatSelectiveStreamReader::intToList).setNullValuesFrequency(SOME));
    }

    // Some of the maps are null
    @Test
    public void testWithNulls()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_some_null_maps.dwrf",
                INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setNullRowsFrequency(SOME));
    }

    // All maps are null
    @Test
    public void testWithAllNulls()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_all_null_maps.dwrf",
                INTEGER,
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

    // Some maps are empty
    @Test
    public void testWithEmptyMaps()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_some_empty_maps.dwrf",
                INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setEmptyMapsFrequency(SOME));
    }

    // All maps are empty
    @Test
    public void testWithAllEmptyMaps()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_all_empty_maps.dwrf",
                INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setEmptyMapsFrequency(ALL));
    }

    // All maps are empty and encoding is not present
    @Test
    public void testWithAllEmptyMapsWithNoEncoding()
            throws Exception
    {
        runTest(
                "test_flat_map/flat_map_all_empty_maps_no_encoding.dwrf",
                INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setEmptyMapsFrequency(ALL));
    }

    @Test
    public void testMixedEncodings()
            throws Exception
    {
        // Values associated with one key are direct encoded, and all other keys are
        // dictionary encoded.  The dictionary encoded values have occasional values that only appear once
        // to ensure the IN_DICTIONARY stream is present, which means the checkpoints for dictionary encoded
        // values will have a different number of positions compared to direct encoded values.
        runTest("test_flat_map/flat_map_mixed_encodings.dwrf",
                INTEGER,
                ExpectedValuesBuilder.get(Function.identity()).setMixedEncodings());
    }

    @Test
    public void testIntegerWithMissingSequences()
            throws Exception
    {
        // The additional sequences IDs for a flat map aren't a consecutive range [1,N], the odd
        // sequence IDs have been removed.  This is to simulate the case where a file has been modified to delete
        // certain keys from the map by dropping the ColumnEncodings and the associated data.
        runTest("test_flat_map/flat_map_int_missing_sequences.dwrf",
                INTEGER,
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

        Type mapType = mapType(keyType, valueType);

        OrcPredicate orcPredicate = createOrcPredicate(0, mapType, expectedValues, OrcTester.Format.DWRF, true);

        runTest(testOrcFileName, mapType, expectedValues, orcPredicate, Optional.empty(), ImmutableList.of());
        runTest(testOrcFileName, mapType, expectedValues.stream().filter(Objects::isNull).collect(toList()), orcPredicate, Optional.of(IS_NULL), ImmutableList.of());
        runTest(testOrcFileName, mapType, expectedValues.stream().filter(Objects::nonNull).collect(toList()), orcPredicate, Optional.of(IS_NOT_NULL), ImmutableList.of());

        if (keyType != VARBINARY) {
            // read only some keys
            List<K> keys = expectedValues.stream().filter(Objects::nonNull).flatMap(v -> v.keySet().stream()).distinct().collect(toImmutableList());
            if (!keys.isEmpty()) {
                List<K> requiredKeys = ImmutableList.of(keys.get(0));
                runTest(testOrcFileName, mapType, pruneMaps(expectedValues, requiredKeys), orcPredicate, Optional.empty(), toSubfields(keyType, requiredKeys));

                List<Integer> keyIndices = ImmutableList.of(1, 3, 7, 11);
                requiredKeys = keyIndices.stream().filter(k -> k < keys.size()).map(keys::get).collect(toList());
                runTest(testOrcFileName, mapType, pruneMaps(expectedValues, requiredKeys), orcPredicate, Optional.empty(), toSubfields(keyType, requiredKeys));
            }
        }

        // read only some rows
        List<Integer> ids = IntStream.range(0, expectedValues.size()).map(i -> i % 10).boxed().collect(toImmutableList());
        ImmutableList<Type> types = ImmutableList.of(mapType, INTEGER);

        Map<Integer, Map<Subfield, TupleDomainFilter>> filters = ImmutableMap.of(1, ImmutableMap.of(new Subfield("c"), toBigintValues(new long[] {1, 5, 6}, true)));
        assertFileContentsPresto(
                types,
                new File(getResource(testOrcFileName).getFile()),
                filterRows(types, ImmutableList.of(expectedValues, ids), filters),
                OrcEncoding.DWRF,
                OrcPredicate.TRUE,
                Optional.of(filters),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of());

        TestingFilterFunction filterFunction = new TestingFilterFunction(mapType);
        assertFileContentsPresto(
                types,
                new File(getResource(testOrcFileName).getFile()),
                filterFunction.filterRows(ImmutableList.of(expectedValues, ids)),
                OrcEncoding.DWRF,
                OrcPredicate.TRUE,
                Optional.empty(),
                ImmutableList.of(filterFunction),
                ImmutableMap.of(0, 0),
                ImmutableMap.of());
    }

    private static <K, V> List<Map<K, V>> pruneMaps(List<Map<K, V>> maps, List<K> keys)
    {
        return maps.stream()
                .map(map -> map == null ? null : Maps.filterKeys(map, keys::contains))
                .collect(toList());
    }

    private static <K> List<Subfield> toSubfields(Type keyType, List<K> keys)
    {
        if (keyType == TINYINT || keyType == SMALLINT || keyType == INTEGER || keyType == BIGINT) {
            return keys.stream()
                    .map(Number.class::cast)
                    .mapToLong(Number::longValue)
                    .mapToObj(key -> new Subfield.LongSubscript(key))
                    .map(subscript -> new Subfield("c", ImmutableList.of(subscript)))
                    .collect(toImmutableList());
        }

        if (keyType == VARCHAR) {
            return keys.stream()
                    .map(String.class::cast)
                    .map(key -> new Subfield.StringSubscript(key))
                    .map(subscript -> new Subfield("c", ImmutableList.of(subscript)))
                    .collect(toImmutableList());
        }

        throw new UnsupportedOperationException("Unsupported key type: " + keyType);
    }

    private <K, V> void runTest(String testOrcFileName, Type mapType, List<Map<K, V>> expectedValues, OrcPredicate orcPredicate, Optional<TupleDomainFilter> filter, List<Subfield> subfields)
            throws Exception
    {
        List<Type> types = ImmutableList.of(mapType);
        Optional<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters = filter.map(f -> ImmutableMap.of(new Subfield("c"), f))
                .map(f -> ImmutableMap.of(0, f));

        assertFileContentsPresto(
                types,
                new File(getResource(testOrcFileName).getFile()),
                filters.map(f -> filterRows(types, ImmutableList.of(expectedValues), f)).orElse(ImmutableList.of(expectedValues)),
                OrcEncoding.DWRF,
                orcPredicate,
                filters,
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(0, subfields));
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
            ALL_EXCEPT_FIRST
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

    static class TestingFilterFunction
            extends FilterFunction
    {
        private static final Session TEST_SESSION = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        private final Type mapType;

        public TestingFilterFunction(final Type mapType)
        {
            super(TEST_SESSION.getSqlFunctionProperties(), true, new Predicate()
            {
                @Override
                public int[] getInputChannels()
                {
                    return new int[] {0};
                }

                @Override
                public boolean evaluate(SqlFunctionProperties properties, Page page, int position)
                {
                    Block mapBlock = page.getBlock(0);
                    if (mapBlock.isNull(position)) {
                        return false;
                    }
                    Map map = (Map) mapType.getObjectValue(TEST_SESSION.getSqlFunctionProperties(), mapBlock, position);
                    return map.containsKey(1);
                }
            });
            this.mapType = mapType;
        }

        public List<List<?>> filterRows(List<List<?>> values)
        {
            List<Integer> passingRows = IntStream.range(0, values.get(0).size())
                    .filter(row -> values.get(0).get(row) != null)
                    .filter(row -> ((Map) values.get(0).get(row)).containsKey(1))
                    .boxed()
                    .collect(toList());
            return IntStream.range(0, values.size())
                    .mapToObj(column -> passingRows.stream().map(values.get(column)::get).collect(toList()))
                    .collect(toList());
        }
    }
}
