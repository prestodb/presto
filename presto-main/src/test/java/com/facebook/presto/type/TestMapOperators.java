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
package com.facebook.presto.type;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.util.StructuralTestUtil.appendToBlockBuilder;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.builder;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class TestMapOperators
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
    }

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Slice uncheckedToJson(@SqlType("varchar(x)") Slice slice)
    {
        return slice;
    }

    @Test
    public void testConstructor()
    {
        assertFunction("MAP(ARRAY ['1','3'], ARRAY [2,4])", mapType(createVarcharType(1), INTEGER), ImmutableMap.of("1", 2, "3", 4));
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 2);
        map.put(3, null);
        assertFunction("MAP(ARRAY [1, 3], ARRAY[2, NULL])", mapType(INTEGER, INTEGER), map);
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2.0E0, 4.0E0])", mapType(INTEGER, DOUBLE), ImmutableMap.of(1, 2.0, 3, 4.0));
        assertFunction(
                "MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3])",
                mapType(createDecimalType(29, 14), createDecimalType(2, 1)),
                ImmutableMap.of(decimal("000000000000001.00000000000000"), decimal("2.2"), decimal("383838383838383.12324234234234"), decimal("3.3")));
        assertFunction("MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ ARRAY[BIGINT '1', BIGINT '2'], ARRAY[ BIGINT '3' ]])",
                mapType(DOUBLE, new ArrayType(BIGINT)),
                ImmutableMap.of(1.0, ImmutableList.of(1L, 2L), 2.0, ImmutableList.of(3L)));
        assertFunction("MAP(ARRAY['puppies'], ARRAY['kittens'])", mapType(createVarcharType(7), createVarcharType(7)), ImmutableMap.of("puppies", "kittens"));
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY[2,4])", mapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 2, false, 4));
        assertFunction(
                "MAP(ARRAY['1', '100'], ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'])",
                mapType(createVarcharType(3), TIMESTAMP),
                ImmutableMap.of(
                        "1",
                        sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, TEST_SESSION),
                        "100",
                        sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, TEST_SESSION)));
        assertFunction(
                "MAP(ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'], ARRAY[1.0E0, 100.0E0])",
                mapType(TIMESTAMP, DOUBLE),
                ImmutableMap.of(
                        sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, TEST_SESSION),
                        1.0,
                        sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, TEST_SESSION),
                        100.0));

        assertInvalidFunction("MAP(ARRAY [1], ARRAY [2, 4])", "Key and value arrays must be the same length");
        assertInvalidFunction("MAP(ARRAY [1, 2, 3, 2], ARRAY [4, 5, 6, 7])", "Duplicate map keys (2) are not allowed");
        assertInvalidFunction(
                "MAP(ARRAY [ARRAY [1, 2], ARRAY [1, 3], ARRAY [1, 2]], ARRAY [1, 2, 3])",
                "Duplicate map keys ([1, 2]) are not allowed");

        assertCachedInstanceHasBoundedRetainedSize("MAP(ARRAY ['1','3'], ARRAY [2,4])");

        assertFunction("MAP(ARRAY [ARRAY[1]], ARRAY[2])", mapType(new ArrayType(INTEGER), INTEGER), ImmutableMap.of(ImmutableList.of(1), 2));
        assertInvalidFunction("MAP(ARRAY [NULL], ARRAY[2])", "map key cannot be null");
        assertInvalidFunction("MAP(ARRAY [ARRAY[NULL]], ARRAY[2])", "map key cannot be indeterminate: [null]");
    }

    @Test
    public void testEmptyMapConstructor()
    {
        assertFunction("MAP()", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
    }

    @Test
    public void testCardinality()
    {
        assertFunction("CARDINALITY(MAP(ARRAY ['1','3'], ARRAY [2,4]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY [1, 3], ARRAY[2, NULL]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY [1, 3], ARRAY [2.0E0, 4.0E0]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ ARRAY[1, 2], ARRAY[3]]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY['puppies'], ARRAY['kittens']))", BIGINT, 1L);
        assertFunction("CARDINALITY(MAP(ARRAY[TRUE], ARRAY[2]))", BIGINT, 1L);
        assertFunction("CARDINALITY(MAP(ARRAY['1'], ARRAY[from_unixtime(1)]))", BIGINT, 1L);
        assertFunction("CARDINALITY(MAP(ARRAY[from_unixtime(1)], ARRAY[1.0E0]))", BIGINT, 1L);
        assertFunction("CARDINALITY(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY [1.0], ARRAY [2.2]))", BIGINT, 1L);
    }

    @Test
    public void testMapToJson()
    {
        // Test key ordering
        assertFunction("CAST(MAP(ARRAY[7,5,3,1], ARRAY[8,6,4,2]) AS JSON)", JSON, "{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");
        assertFunction("CAST(MAP(ARRAY[1,3,5,7], ARRAY[2,4,6,8]) AS JSON)", JSON, "{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");

        // Test null value
        assertFunction("cast(cast (null as MAP<BIGINT, BIGINT>) AS JSON)", JSON, null);
        assertFunction("cast(MAP() AS JSON)", JSON, "{}");
        assertFunction("cast(MAP(ARRAY[1, 2], ARRAY[null, null]) AS JSON)", JSON, "{\"1\":null,\"2\":null}");

        // Test key types
        assertFunction("CAST(MAP(ARRAY[true, false], ARRAY[1, 2]) AS JSON)", JSON, "{\"false\":2,\"true\":1}");

        assertFunction(
                "cast(MAP(cast(ARRAY[1, 2, 3] AS ARRAY<TINYINT>), ARRAY[5, 8, null]) AS JSON)",
                JSON,
                "{\"1\":5,\"2\":8,\"3\":null}");
        assertFunction(
                "cast(MAP(cast(ARRAY[12345, 12346, 12347] AS ARRAY<SMALLINT>), ARRAY[5, 8, null]) AS JSON)",
                JSON,
                "{\"12345\":5,\"12346\":8,\"12347\":null}");
        assertFunction(
                "cast(MAP(cast(ARRAY[123456789,123456790,123456791] AS ARRAY<INTEGER>), ARRAY[5, 8, null]) AS JSON)",
                JSON,
                "{\"123456789\":5,\"123456790\":8,\"123456791\":null}");
        assertFunction(
                "cast(MAP(cast(ARRAY[1234567890123456111,1234567890123456222,1234567890123456777] AS ARRAY<BIGINT>), ARRAY[111, 222, null]) AS JSON)",
                JSON,
                "{\"1234567890123456111\":111,\"1234567890123456222\":222,\"1234567890123456777\":null}");

        assertFunction(
                "cast(MAP(cast(ARRAY[3.14E0, 1e10, 1e20] AS ARRAY<REAL>), ARRAY[null, 10, 20]) AS JSON)",
                JSON,
                "{\"1.0E10\":10,\"1.0E20\":20,\"3.14\":null}");

        assertFunction(
                "cast(MAP(ARRAY[1e-323,1e308,nan()], ARRAY[-323,308,null]) AS JSON)",
                JSON,
                "{\"1.0E-323\":-323,\"1.0E308\":308,\"NaN\":null}");
        assertFunction(
                "cast(MAP(ARRAY[DECIMAL '3.14', DECIMAL '0.01'], ARRAY[0.14, null]) AS JSON)",
                JSON,
                "{\"0.01\":null,\"3.14\":0.14}");

        assertFunction(
                "cast(MAP(ARRAY[DECIMAL '12345678901234567890.1234567890666666', DECIMAL '0.0'], ARRAY[666666, null]) AS JSON)",
                JSON,
                "{\"0.0000000000000000\":null,\"12345678901234567890.1234567890666666\":666666}");

        assertFunction("CAST(MAP(ARRAY['a', 'bb', 'ccc'], ARRAY[1, 2, 3]) AS JSON)", JSON, "{\"a\":1,\"bb\":2,\"ccc\":3}");

        // Test value types
        assertFunction("cast(MAP(ARRAY[1, 2, 3], ARRAY[true, false, null]) AS JSON)", JSON, "{\"1\":true,\"2\":false,\"3\":null}");

        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[5, 8, null] AS ARRAY<TINYINT>)) AS JSON)",
                JSON,
                "{\"1\":5,\"2\":8,\"3\":null}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[12345, -12345, null] AS ARRAY<SMALLINT>)) AS JSON)",
                JSON,
                "{\"1\":12345,\"2\":-12345,\"3\":null}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[123456789, -123456789, null] AS ARRAY<INTEGER>)) AS JSON)",
                JSON,
                "{\"1\":123456789,\"2\":-123456789,\"3\":null}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY<BIGINT>)) AS JSON)",
                JSON,
                "{\"1\":1234567890123456789,\"2\":-1234567890123456789,\"3\":null}");

        assertFunction(
                "CAST(MAP(ARRAY[1, 2, 3, 5, 8], CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY<REAL>)) AS JSON)",
                JSON,
                "{\"1\":3.14,\"2\":\"NaN\",\"3\":\"Infinity\",\"5\":\"-Infinity\",\"8\":null}");
        assertFunction(
                "CAST(MAP(ARRAY[1, 2, 3, 5, 8, 13, 21], ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null]) AS JSON)",
                JSON,
                "{\"1\":3.14,\"13\":\"-Infinity\",\"2\":1.0E-323,\"21\":null,\"3\":1.0E308,\"5\":\"NaN\",\"8\":\"Infinity\"}");
        assertFunction("CAST(MAP(ARRAY[1, 2], ARRAY[DECIMAL '3.14', null]) AS JSON)", JSON, "{\"1\":3.14,\"2\":null}");
        assertFunction(
                "CAST(MAP(ARRAY[1, 2], ARRAY[DECIMAL '12345678901234567890.123456789012345678', null]) AS JSON)",
                JSON,
                "{\"1\":12345678901234567890.123456789012345678,\"2\":null}");

        assertFunction("cast(MAP(ARRAY[1, 2, 3], ARRAY['a', 'bb', null]) AS JSON)", JSON, "{\"1\":\"a\",\"2\":\"bb\",\"3\":null}");
        assertFunction(
                "CAST(MAP(ARRAY[1, 2, 3, 5, 8, 13, 21, 34], ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null]) AS JSON)",
                JSON,
                "{\"1\":123,\"13\":{\"a\":1,\"b\":\"str\",\"c\":null},\"2\":3.14,\"21\":null,\"3\":false,\"34\":null,\"5\":\"abc\",\"8\":[1,\"a\",null]}");

        assertFunction(
                "CAST(MAP(ARRAY[1, 2], ARRAY[TIMESTAMP '1970-01-01 00:00:01', null]) AS JSON)",
                JSON,
                format("{\"1\":\"%s\",\"2\":null}", sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, TEST_SESSION).toString()));
        assertFunction(
                "CAST(MAP(ARRAY[2, 5, 3], ARRAY[DATE '2001-08-22', DATE '2001-08-23', null]) AS JSON)",
                JSON,
                "{\"2\":\"2001-08-22\",\"3\":null,\"5\":\"2001-08-23\"}");

        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3, 5, 8], ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null]) AS JSON)",
                JSON,
                "{\"1\":[1,2],\"2\":[3,null],\"3\":[],\"5\":[null,null],\"8\":null}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 8, 5, 3], ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null]) AS JSON)",
                JSON,
                "{\"1\":{\"a\":1,\"b\":2},\"2\":{\"none\":null,\"three\":3},\"3\":null,\"5\":{\"h1\":null,\"h2\":null},\"8\":{}}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3, 5], ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null]) AS JSON)",
                JSON,
                "{\"1\":[1,2],\"2\":[3,null],\"3\":[null,null],\"5\":null}");
        assertFunction("CAST(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) AS JSON)", JSON, "{\"1.00000000000000\":2.2,\"383838383838383.12324234234234\":3.3}");
        assertFunction("CAST(MAP(ARRAY [1.0], ARRAY [2.2]) AS JSON)", JSON, "{\"1.0\":2.2}");
    }

    @Test
    public void testJsonToMap()
    {
        // special values
        assertFunction("CAST(CAST (null AS JSON) AS MAP<BIGINT, BIGINT>)", mapType(BIGINT, BIGINT), null);
        assertFunction("CAST(JSON 'null' AS MAP<BIGINT, BIGINT>)", mapType(BIGINT, BIGINT), null);
        assertFunction("CAST(JSON '{}' AS MAP<BIGINT, BIGINT>)", mapType(BIGINT, BIGINT), ImmutableMap.of());
        assertFunction("CAST(JSON '{\"1\": null, \"2\": null}' AS MAP<BIGINT, BIGINT>)",
                mapType(BIGINT, BIGINT),
                asMap(ImmutableList.of(1L, 2L), asList(null, null)));
        assertInvalidCast("CAST(CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) AS JSON) AS MAP<DECIMAL(10,5), DECIMAL(2,1)>)");

        // key type: boolean
        assertFunction("CAST(JSON '{\"true\": 1, \"false\": 0}' AS MAP<BOOLEAN, BIGINT>)",
                mapType(BOOLEAN, BIGINT),
                ImmutableMap.of(true, 1L, false, 0L));

        // key type: tinyint, smallint, integer, bigint
        assertFunction("CAST(JSON '{\"1\": 5, \"2\": 8, \"3\": 13}' AS MAP<TINYINT, BIGINT>)",
                mapType(TINYINT, BIGINT),
                ImmutableMap.of((byte) 1, 5L, (byte) 2, 8L, (byte) 3, 13L));
        assertFunction("CAST(JSON '{\"12345\": 5, \"12346\": 8, \"12347\": 13}' AS MAP<SMALLINT, BIGINT>)",
                mapType(SMALLINT, BIGINT),
                ImmutableMap.of((short) 12345, 5L, (short) 12346, 8L, (short) 12347, 13L));
        assertFunction("CAST(JSON '{\"123456789\": 5, \"123456790\": 8, \"123456791\": 13}' AS MAP<INTEGER, BIGINT>)",
                mapType(INTEGER, BIGINT),
                ImmutableMap.of(123456789, 5L, 123456790, 8L, 123456791, 13L));
        assertFunction("CAST(JSON '{\"1234567890123456111\": 5, \"1234567890123456222\": 8, \"1234567890123456777\": 13}' AS MAP<BIGINT, BIGINT>)",
                mapType(BIGINT, BIGINT),
                ImmutableMap.of(1234567890123456111L, 5L, 1234567890123456222L, 8L, 1234567890123456777L, 13L));

        // key type: real, double, decimal
        assertFunction("CAST(JSON '{\"3.14\": 5, \"NaN\": 8, \"Infinity\": 13, \"-Infinity\": 21}' AS MAP<REAL, BIGINT>)",
                mapType(REAL, BIGINT),
                ImmutableMap.of(3.14f, 5L, Float.NaN, 8L, Float.POSITIVE_INFINITY, 13L, Float.NEGATIVE_INFINITY, 21L));
        assertFunction("CAST(JSON '{\"3.1415926\": 5, \"NaN\": 8, \"Infinity\": 13, \"-Infinity\": 21}' AS MAP<DOUBLE, BIGINT>)",
                mapType(DOUBLE, BIGINT),
                ImmutableMap.of(3.1415926, 5L, Double.NaN, 8L, Double.POSITIVE_INFINITY, 13L, Double.NEGATIVE_INFINITY, 21L));
        assertFunction("CAST(JSON '{\"123.456\": 5, \"3.14\": 8}' AS MAP<DECIMAL(10, 5), BIGINT>)",
                mapType(createDecimalType(10, 5), BIGINT),
                ImmutableMap.of(decimal("123.45600"), 5L, decimal("3.14000"), 8L));
        assertFunction("CAST(JSON '{\"12345678.12345678\": 5, \"3.1415926\": 8}' AS MAP<DECIMAL(38, 8), BIGINT>)",
                mapType(createDecimalType(38, 8), BIGINT),
                ImmutableMap.of(decimal("12345678.12345678"), 5L, decimal("3.14159260"), 8L));

        // key type: varchar
        assertFunction("CAST(JSON '{\"a\": 5, \"bb\": 8, \"ccc\": 13}' AS MAP<VARCHAR, BIGINT>)",
                mapType(VARCHAR, BIGINT),
                ImmutableMap.of("a", 5L, "bb", 8L, "ccc", 13L));

        // value type: boolean
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 0, \"8\": 12.3, \"13\": 0.0, \"21\": \"true\", \"34\": \"false\", \"55\": null}' AS MAP<BIGINT, BOOLEAN>)",
                mapType(BIGINT, BOOLEAN),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L),
                        asList(true, false, true, false, true, false, true, false, null)));

        // value type: tinyint, smallint, integer, bigint
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 12.7, \"8\": \"12\", \"13\": null}' AS MAP<BIGINT, TINYINT>)",
                mapType(BIGINT, TINYINT),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList((byte) 1, (byte) 0, (byte) 12, (byte) 13, (byte) 12, null)));
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12345, \"5\": 12345.6, \"8\": \"12345\", \"13\": null}' AS MAP<BIGINT, SMALLINT>)",
                mapType(BIGINT, SMALLINT),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList((short) 1, (short) 0, (short) 12345, (short) 12346, (short) 12345, null)));
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12345678, \"5\": 12345678.9, \"8\": \"12345678\", \"13\": null}' AS MAP<BIGINT, INTEGER>)",
                mapType(BIGINT, INTEGER),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList(1, 0, 12345678, 12345679, 12345678, null)));
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 1234567891234567, \"5\": 1234567891234567.8, \"8\": \"1234567891234567\", \"13\": null}' AS MAP<BIGINT, BIGINT>)",
                mapType(BIGINT, BIGINT),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList(1L, 0L, 1234567891234567L, 1234567891234568L, 1234567891234567L, null)));

        // value type: real, double, decimal
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12345, \"5\": 12345.67, \"8\": \"3.14\", \"13\": \"NaN\", \"21\": \"Infinity\", \"34\": \"-Infinity\", \"55\": null}' AS MAP<BIGINT, REAL>)",
                mapType(BIGINT, REAL),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L),
                        asList(1.0f, 0.0f, 12345.0f, 12345.67f, 3.14f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, null)));
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 1234567890, \"5\": 1234567890.1, \"8\": \"3.14\", \"13\": \"NaN\", \"21\": \"Infinity\", \"34\": \"-Infinity\", \"55\": null}' AS MAP<BIGINT, DOUBLE>)",
                mapType(BIGINT, DOUBLE),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L),
                        asList(1.0, 0.0, 1234567890.0, 1234567890.1, 3.14, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null)));
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 128, \"5\": 123.456, \"8\": \"3.14\", \"13\": null}' AS MAP<BIGINT, DECIMAL(10, 5)>)",
                mapType(BIGINT, createDecimalType(10, 5)),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList(decimal("1.00000"), decimal("0.00000"), decimal("128.00000"), decimal("123.45600"), decimal("3.14000"), null)));
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 128, \"5\": 12345678.12345678, \"8\": \"3.14\", \"13\": null}' AS MAP<BIGINT, DECIMAL(38, 8)>)",
                mapType(BIGINT, createDecimalType(38, 8)),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList(decimal("1.00000000"), decimal("0.00000000"), decimal("128.00000000"), decimal("12345678.12345678"), decimal("3.14000000"), null)));

        // varchar, json
        assertFunction("CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 12.3, \"8\": \"puppies\", \"13\": \"kittens\", \"21\": \"null\", \"34\": \"\", \"55\": null}' AS MAP<BIGINT, VARCHAR>)",
                mapType(BIGINT, VARCHAR),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L),
                        asList("true", "false", "12", "12.3", "puppies", "kittens", "null", "", null)));

        assertFunction("CAST(JSON '{\"k1\": 5, \"k2\": 3.14, \"k3\":[1, 2, 3], \"k4\":\"e\", \"k5\":{\"a\": \"b\"}, \"k6\":null, \"k7\":\"null\", \"k8\":[null]}' AS MAP<VARCHAR, JSON>)",
                mapType(VARCHAR, JSON),
                builder()
                        .put("k1", "5")
                        .put("k2", "3.14")
                        .put("k3", "[1,2,3]")
                        .put("k4", "\"e\"")
                        .put("k5", "{\"a\":\"b\"}")
                        .put("k6", "null")
                        .put("k7", "\"null\"")
                        .put("k8", "[null]")
                        .build());

        // These two tests verifies that partial json cast preserves input order
        // The second test should never happen in real life because valid json in presto requires natural key ordering.
        // However, it is added to make sure that the order in the first test is not a coincidence.
        assertFunction("CAST(JSON '{\"k1\": {\"1klmnopq\":1, \"2klmnopq\":2, \"3klmnopq\":3, \"4klmnopq\":4, \"5klmnopq\":5, \"6klmnopq\":6, \"7klmnopq\":7}}' AS MAP<VARCHAR, JSON>)",
                mapType(VARCHAR, JSON),
                ImmutableMap.of("k1", "{\"1klmnopq\":1,\"2klmnopq\":2,\"3klmnopq\":3,\"4klmnopq\":4,\"5klmnopq\":5,\"6klmnopq\":6,\"7klmnopq\":7}"));
        assertFunction("CAST(unchecked_to_json('{\"k1\": {\"7klmnopq\":7, \"6klmnopq\":6, \"5klmnopq\":5, \"4klmnopq\":4, \"3klmnopq\":3, \"2klmnopq\":2, \"1klmnopq\":1}}') AS MAP<VARCHAR, JSON>)",
                mapType(VARCHAR, JSON),
                ImmutableMap.of("k1", "{\"7klmnopq\":7,\"6klmnopq\":6,\"5klmnopq\":5,\"4klmnopq\":4,\"3klmnopq\":3,\"2klmnopq\":2,\"1klmnopq\":1}"));

        // nested array/map
        assertFunction("CAST(JSON '{\"1\": [1, 2], \"2\": [3, null], \"3\": [], \"5\": [null, null], \"8\": null}' AS MAP<BIGINT, ARRAY<BIGINT>>)",
                mapType(BIGINT, new ArrayType(BIGINT)),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L),
                        asList(asList(1L, 2L), asList(3L, null), emptyList(), asList(null, null), null)));

        assertFunction("CAST(JSON '{" +
                        "\"1\": {\"a\": 1, \"b\": 2}, " +
                        "\"2\": {\"none\": null, \"three\": 3}, " +
                        "\"3\": {}, " +
                        "\"5\": {\"h1\": null,\"h2\": null}, " +
                        "\"8\": null}' " +
                        "AS MAP<BIGINT, MAP<VARCHAR, BIGINT>>)",
                mapType(BIGINT, mapType(VARCHAR, BIGINT)),
                asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L),
                        asList(
                                ImmutableMap.of("a", 1L, "b", 2L),
                                asMap(ImmutableList.of("none", "three"), asList(null, 3L)),
                                ImmutableMap.of(),
                                asMap(ImmutableList.of("h1", "h2"), asList(null, null)),
                                null)));

        assertFunction("CAST(JSON '{" +
                        "\"row1\": [1, \"two\"], " +
                        "\"row2\": [3, null], " +
                        "\"row3\": {\"k1\": 1, \"k2\": \"two\"}, " +
                        "\"row4\": {\"k2\": null, \"k1\": 3}, " +
                        "\"row5\": null}' " +
                        "AS MAP<VARCHAR, ROW(k1 BIGINT, k2 VARCHAR)>)",
                mapType(VARCHAR, RowType.from(ImmutableList.of(
                        RowType.field("k1", BIGINT),
                        RowType.field("k2", VARCHAR)))),
                asMap(
                        ImmutableList.of("row1", "row2", "row3", "row4", "row5"),
                        asList(
                                asList(1L, "two"),
                                asList(3L, null),
                                asList(1L, "two"),
                                asList(3L, null),
                                null)));

        // invalid cast
        assertInvalidCast("CAST(JSON '{\"[]\": 1}' AS MAP<ARRAY<BIGINT>, BIGINT>)", "Cannot cast JSON to map(array(bigint),bigint)");

        assertInvalidCast("CAST(JSON '[1, 2]' AS MAP<BIGINT, BIGINT>)", "Cannot cast to map(bigint,bigint). Expected a json object, but got [\n[1,2]");
        assertInvalidCast("CAST(JSON '{\"a\": 1, \"b\": 2}' AS MAP<VARCHAR, MAP<VARCHAR, BIGINT>>)", "Cannot cast to map(varchar,map(varchar,bigint)). Expected a json object, but got 1\n{\"a\":1,\"b\":2}");
        assertInvalidCast("CAST(JSON '{\"a\": 1, \"b\": []}' AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map(varchar,bigint). Unexpected token when cast to bigint: [\n{\"a\":1,\"b\":[]}");
        assertInvalidCast("CAST(JSON '{\"1\": {\"a\": 1}, \"2\": []}' AS MAP<VARCHAR, MAP<VARCHAR, BIGINT>>)", "Cannot cast to map(varchar,map(varchar,bigint)). Expected a json object, but got [\n{\"1\":{\"a\":1},\"2\":[]}");

        assertInvalidCast("CAST(unchecked_to_json('\"a\": 1, \"b\": 2') AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map(varchar,bigint). Expected a json object, but got a\n\"a\": 1, \"b\": 2");
        assertInvalidCast("CAST(unchecked_to_json('{\"a\": 1} 2') AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map(varchar,bigint). Unexpected trailing token: 2\n{\"a\": 1} 2");
        assertInvalidCast("CAST(unchecked_to_json('{\"a\": 1') AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map(varchar,bigint).\n{\"a\": 1");

        assertInvalidCast("CAST(JSON '{\"a\": \"b\"}' AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map(varchar,bigint). Cannot cast 'b' to BIGINT\n{\"a\":\"b\"}");
        assertInvalidCast("CAST(JSON '{\"a\": 1234567890123.456}' AS MAP<VARCHAR, INTEGER>)", "Cannot cast to map(varchar,integer). Out of range for integer: 1.234567890123456E12\n{\"a\":1.234567890123456E12}");

        assertInvalidCast("CAST(JSON '{\"1\":1, \"01\": 2}' AS MAP<BIGINT, BIGINT>)", "Cannot cast to map(bigint,bigint). Duplicate keys are not allowed\n{\"01\":2,\"1\":1}");
        assertInvalidCast("CAST(JSON '[{\"1\":1, \"01\": 2}]' AS ARRAY<MAP<BIGINT, BIGINT>>)", "Cannot cast to array(map(bigint,bigint)). Duplicate keys are not allowed\n[{\"01\":2,\"1\":1}]");

        // some other key/value type combinations
        assertFunction("CAST(JSON '{\"puppies\":\"kittens\"}' AS MAP<VARCHAR, VARCHAR>)",
                mapType(VARCHAR, VARCHAR),
                ImmutableMap.of("puppies", "kittens"));
        assertFunction("CAST(JSON '{\"true\":\"kittens\"}' AS MAP<BOOLEAN, VARCHAR>)",
                mapType(BOOLEAN, VARCHAR),
                ImmutableMap.of(true, "kittens"));
        assertFunction("CAST(JSON 'null' AS MAP<BOOLEAN, VARCHAR>)",
                mapType(BOOLEAN, VARCHAR),
                null);
        // cannot use JSON literal containing DECIMAL values right now.
        // Decimal literal are interpreted internally by JSON parser as double and precision is lost.

        assertFunction(
                "CAST(CAST(MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3]) AS JSON) AS MAP<DECIMAL(29,14), DECIMAL(2,1)>)",
                mapType(createDecimalType(29, 14), createDecimalType(2, 1)),
                ImmutableMap.of(decimal("000000000000001.00000000000000"), decimal("2.2"), decimal("383838383838383.12324234234234"), decimal("3.3")));
        assertFunction(
                "CAST(CAST(MAP(ARRAY[2.2, 3.3], ARRAY[1.0, 383838383838383.12324234234234]) AS JSON) AS MAP<DECIMAL(2,1), DECIMAL(29,14)>)",
                mapType(createDecimalType(2, 1), createDecimalType(29, 14)),
                ImmutableMap.of(decimal("2.2"), decimal("000000000000001.00000000000000"), decimal("3.3"), decimal("383838383838383.12324234234234")));
        assertInvalidCast("CAST(CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) AS JSON) AS MAP<DECIMAL(2,1), DECIMAL(10,5)>)");
        assertInvalidCast("CAST(CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) AS JSON) AS MAP<DECIMAL(10,5), DECIMAL(2,1)>)");
    }

    @Test
    public void testElementAt()
    {
        // empty map
        assertFunction("element_at(MAP(CAST(ARRAY [] AS ARRAY(BIGINT)), CAST(ARRAY [] AS ARRAY(BIGINT))), 1)", BIGINT, null);

        // missing key
        assertFunction("element_at(MAP(ARRAY [1], ARRAY [1e0]), 2)", DOUBLE, null);
        assertFunction("element_at(MAP(ARRAY [1.0], ARRAY ['a']), 2.0)", createVarcharType(1), null);
        assertFunction("element_at(MAP(ARRAY ['a'], ARRAY [true]), 'b')", BOOLEAN, null);
        assertFunction("element_at(MAP(ARRAY [true], ARRAY [ARRAY [1]]), false)", new ArrayType(INTEGER), null);
        assertFunction("element_at(MAP(ARRAY [ARRAY [1]], ARRAY [1]), ARRAY [2])", INTEGER, null);

        // null value associated with the requested key
        assertFunction("element_at(MAP(ARRAY [1], ARRAY [null]), 1)", UNKNOWN, null);
        assertFunction("element_at(MAP(ARRAY [1.0E0], ARRAY [null]), 1.0E0)", UNKNOWN, null);
        assertFunction("element_at(MAP(ARRAY [TRUE], ARRAY [null]), TRUE)", UNKNOWN, null);
        assertFunction("element_at(MAP(ARRAY ['puppies'], ARRAY [null]), 'puppies')", UNKNOWN, null);
        assertFunction("element_at(MAP(ARRAY [ARRAY [1]], ARRAY [null]), ARRAY [1])", UNKNOWN, null);

        // general tests
        assertFunction("element_at(MAP(ARRAY [1, 3], ARRAY [2, 4]), 3)", INTEGER, 4);
        assertFunction("element_at(MAP(ARRAY [BIGINT '1', 3], ARRAY [BIGINT '2', 4]), 3)", BIGINT, 4L);
        assertFunction("element_at(MAP(ARRAY [1, 3], ARRAY [2, NULL]), 3)", INTEGER, null);
        assertFunction("element_at(MAP(ARRAY [BIGINT '1', 3], ARRAY [2, NULL]), 3)", INTEGER, null);
        assertFunction("element_at(MAP(ARRAY [1, 3], ARRAY [2.0E0, 4.0E0]), 1)", DOUBLE, 2.0);
        assertFunction("element_at(MAP(ARRAY [1.0E0, 2.0E0], ARRAY [ARRAY [1, 2], ARRAY [3]]), 1.0E0)", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("element_at(MAP(ARRAY ['puppies'], ARRAY ['kittens']), 'puppies')", createVarcharType(7), "kittens");
        assertFunction("element_at(MAP(ARRAY [TRUE, FALSE], ARRAY [2, 4]), TRUE)", INTEGER, 2);
        assertFunction("element_at(MAP(ARRAY [ARRAY [1, 2], ARRAY [3]], ARRAY [1e0, 2e0]), ARRAY [1, 2])", DOUBLE, 1.0);
        assertFunction(
                "element_at(MAP(ARRAY ['1', '100'], ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '2005-09-10 13:00:00']), '1')",
                TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, TEST_SESSION));
        assertFunction("element_at(MAP(ARRAY [from_unixtime(1), from_unixtime(100)], ARRAY [1.0E0, 100.0E0]), from_unixtime(1))", DOUBLE, 1.0);
    }

    @Test
    public void testSubscript()
    {
        assertFunction("MAP(ARRAY [1], ARRAY [null])[1]", UNKNOWN, null);
        assertFunction("MAP(ARRAY [1.0E0], ARRAY [null])[1.0E0]", UNKNOWN, null);
        assertFunction("MAP(ARRAY [TRUE], ARRAY [null])[TRUE]", UNKNOWN, null);
        assertFunction("MAP(ARRAY['puppies'], ARRAY [null])['puppies']", UNKNOWN, null);
        assertInvalidFunction("MAP(ARRAY [CAST(null as bigint)], ARRAY [1])", "map key cannot be null");
        assertInvalidFunction("MAP(ARRAY [CAST(null as bigint)], ARRAY [CAST(null as bigint)])", "map key cannot be null");
        assertInvalidFunction("MAP(ARRAY [1,null], ARRAY [null,2])", "map key cannot be null");
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2, 4])[3]", INTEGER, 4);
        assertFunction("MAP(ARRAY [BIGINT '1', 3], ARRAY [BIGINT '2', 4])[3]", BIGINT, 4L);
        assertFunction("MAP(ARRAY [1, 3], ARRAY[2, NULL])[3]", INTEGER, null);
        assertFunction("MAP(ARRAY [BIGINT '1', 3], ARRAY[2, NULL])[3]", INTEGER, null);
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2.0E0, 4.0E0])[1]", DOUBLE, 2.0);
        assertFunction("MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ ARRAY[1, 2], ARRAY[3]])[1.0E0]", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("MAP(ARRAY['puppies'], ARRAY['kittens'])['puppies']", createVarcharType(7), "kittens");
        assertFunction("MAP(ARRAY[TRUE,FALSE],ARRAY[2,4])[TRUE]", INTEGER, 2);
        assertFunction(
                "MAP(ARRAY['1', '100'], ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'])['1']",
                TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, TEST_SESSION));
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0E0, 100.0E0])[from_unixtime(1)]", DOUBLE, 1.0);
        assertInvalidFunction("MAP(ARRAY [BIGINT '1'], ARRAY [BIGINT '2'])[3]", "Key not present in map: 3");
        assertInvalidFunction("MAP(ARRAY ['hi'], ARRAY [2])['missing']", "Key not present in map: missing");
        assertFunction("MAP(ARRAY[array[1,1]], ARRAY['a'])[ARRAY[1,1]]", createVarcharType(1), "a");
        assertFunction("MAP(ARRAY[('a', 'b')], ARRAY[ARRAY[100, 200]])[('a', 'b')]", new ArrayType(INTEGER), ImmutableList.of(100, 200));
        assertFunction("MAP(ARRAY[1.0], ARRAY [2.2])[1.0]", createDecimalType(2, 1), decimal("2.2"));
        assertFunction("MAP(ARRAY[000000000000001.00000000000000], ARRAY [2.2])[000000000000001.00000000000000]", createDecimalType(2, 1), decimal("2.2"));
        assertInvalidFunction("MAP(ARRAY[cast('1' as varbinary)], ARRAY[null])[cast('2' as varbinary)]", "Key not present in map");
    }

    @Test
    public void testMapKeys()
    {
        assertFunction("MAP_KEYS(MAP(ARRAY['1', '3'], ARRAY['2', '4']))", new ArrayType(createVarcharType(1)), ImmutableList.of("1", "3"));
        assertFunction("MAP_KEYS(MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]]))", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0));
        assertFunction("MAP_KEYS(MAP(ARRAY['puppies'], ARRAY['kittens']))", new ArrayType(createVarcharType(7)), ImmutableList.of("puppies"));
        assertFunction("MAP_KEYS(MAP(ARRAY[TRUE], ARRAY[2]))", new ArrayType(BOOLEAN), ImmutableList.of(true));
        assertFunction(
                "MAP_KEYS(MAP(ARRAY[TIMESTAMP '1970-01-01 00:00:01'], ARRAY[1.0E0]))",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, TEST_SESSION)));
        assertFunction("MAP_KEYS(MAP(ARRAY[CAST('puppies' as varbinary)], ARRAY['kittens']))", new ArrayType(VARBINARY), ImmutableList.of(new SqlVarbinary("puppies".getBytes(UTF_8))));
        assertFunction("MAP_KEYS(MAP(ARRAY[1,2],  ARRAY[ARRAY[1, 2], ARRAY[3]]))", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("MAP_KEYS(MAP(ARRAY[1,4], ARRAY[MAP(ARRAY[2], ARRAY[3]), MAP(ARRAY[5], ARRAY[6])]))", new ArrayType(INTEGER), ImmutableList.of(1, 4));
        assertFunction("MAP_KEYS(MAP(ARRAY [ARRAY [1], ARRAY [2, 3]],  ARRAY [ARRAY [3, 4], ARRAY [5]]))", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 3)));
        assertFunction(
                "MAP_KEYS(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]))",
                new ArrayType(createDecimalType(29, 14)),
                ImmutableList.of(decimal("000000000000001.00000000000000"), decimal("383838383838383.12324234234234")));
        assertFunction(
                "MAP_KEYS(MAP(ARRAY [1.0, 2.01], ARRAY [2.2, 3.3]))",
                new ArrayType(createDecimalType(3, 2)),
                ImmutableList.of(decimal("1.00"), decimal("2.01")));
    }

    @Test
    public void testMapValues()
    {
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[ARRAY[TRUE, FALSE, NULL]]))",
                new ArrayType(new ArrayType(BOOLEAN)),
                ImmutableList.of(Lists.newArrayList(true, false, null)));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[ARRAY[ARRAY[1, 2]]]))",
                new ArrayType(new ArrayType(new ArrayType(INTEGER))),
                ImmutableList.of(ImmutableList.of(ImmutableList.of(1, 2))));
        assertFunction("MAP_VALUES(MAP(ARRAY [1, 3], ARRAY ['2', '4']))",
                new ArrayType(createVarcharType(1)),
                ImmutableList.of("2", "4"));
        assertFunction("MAP_VALUES(MAP(ARRAY[1.0E0,2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]]))",
                new ArrayType(new ArrayType(INTEGER)),
                ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));
        assertFunction("MAP_VALUES(MAP(ARRAY['puppies'], ARRAY['kittens']))",
                new ArrayType(createVarcharType(7)),
                ImmutableList.of("kittens"));
        assertFunction("MAP_VALUES(MAP(ARRAY[TRUE], ARRAY[2]))",
                new ArrayType(INTEGER),
                ImmutableList.of(2));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[NULL]))",
                new ArrayType(UNKNOWN),
                Lists.newArrayList((Object) null));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[TRUE]))",
                new ArrayType(BOOLEAN),
                ImmutableList.of(true));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[1.0E0]))",
                new ArrayType(DOUBLE),
                ImmutableList.of(1.0));
        assertFunction("MAP_VALUES(MAP(ARRAY['1', '2'], ARRAY[ARRAY[1.0E0, 2.0E0], ARRAY[3.0E0, 4.0E0]]))",
                new ArrayType(new ArrayType(DOUBLE)),
                ImmutableList.of(ImmutableList.of(1.0, 2.0), ImmutableList.of(3.0, 4.0)));
        assertFunction(
                "MAP_VALUES(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]))",
                new ArrayType(createDecimalType(2, 1)),
                ImmutableList.of(decimal("2.2"), decimal("3.3")));
        assertFunction(
                "MAP_VALUES(MAP(ARRAY [1.0, 2.01], ARRAY [383838383838383.12324234234234, 3.3]))",
                new ArrayType(createDecimalType(29, 14)),
                ImmutableList.of(decimal("383838383838383.12324234234234"), decimal("000000000000003.30000000000000")));
    }

    @Test
    public void testEquals()
    {
        // single item
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1], ARRAY[2])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1], ARRAY[4])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[3], ARRAY[1]) = MAP(ARRAY[2], ARRAY[1])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[2.2], ARRAY[3.1]) = MAP(ARRAY[2.2], ARRAY[3.1])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[2.2], ARRAY[3.1]) = MAP(ARRAY[2.2], ARRAY[3.0])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000013.30000000000000])", BOOLEAN, false);

        // multiple items
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1, 3], ARRAY[2, 4])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[1], ARRAY[2])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[3, 1], ARRAY[4, 2])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[3, 1], ARRAY[2, 4])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0]) = MAP(ARRAY['3', '1'], ARRAY[4.0E0, 2.0E0])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0]) = MAP(ARRAY['3', '1'], ARRAY[2.0E0, 4.0E0])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) = MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) = MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE]) = MAP(ARRAY[3.0E0, 1.0E0], ARRAY[FALSE, TRUE])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE]) = MAP(ARRAY[3.0E0, 1.0E0], ARRAY[TRUE, FALSE])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)]) = MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(100), from_unixtime(1)])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)]) = MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies']) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies']) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[3], ARRAY[1, 2]])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['cat', 'dog']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])", BOOLEAN, false);
        assertFunction("MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) = MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3])", BOOLEAN, true);
        assertFunction("MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) = MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.2])", BOOLEAN, false);

        // nulls
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])", BOOLEAN, null);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[3, 3]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])", BOOLEAN, null);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE]) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL]) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, null]) = MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, null])", BOOLEAN, null);
        assertFunction("MAP(ARRAY [1.0, 2.1], ARRAY [null, null]) = MAP(ARRAY [1.0, 2.1], ARRAY [null, null])", BOOLEAN, null);
    }

    @Test
    public void testNotEquals()
    {
        // single item
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1], ARRAY[2])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1], ARRAY[4])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[3], ARRAY[1]) != MAP(ARRAY[2], ARRAY[1])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[2.2], ARRAY[3.1]) != MAP(ARRAY[2.2], ARRAY[3.1])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[2.2], ARRAY[3.1]) != MAP(ARRAY[2.2], ARRAY[3.0])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "!= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "!= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000013.30000000000000])", BOOLEAN, true);

        // multiple items
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1, 3], ARRAY[2, 4])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[1], ARRAY[2])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[3, 1], ARRAY[4, 2])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[3, 1], ARRAY[2, 4])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0]) != MAP(ARRAY['3', '1'], ARRAY[4.0E0, 2.0E0])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0]) != MAP(ARRAY['3', '1'], ARRAY[2.0E0, 4.0E0])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) != MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) != MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE]) != MAP(ARRAY[3.0E0, 1.0E0], ARRAY[FALSE, TRUE])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE]) != MAP(ARRAY[3.0E0, 1.0E0], ARRAY[TRUE, FALSE])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)]) != MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(100), from_unixtime(1)])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)]) != MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies']) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies']) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) != MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) != MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])", BOOLEAN, true);
        assertFunction("MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) != MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3])", BOOLEAN, false);
        assertFunction("MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) != MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.2])", BOOLEAN, true);

        // nulls
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])", BOOLEAN, null);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[3, 3]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])", BOOLEAN, null);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE]) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL]) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, null]) != MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, null])", BOOLEAN, null);
        assertFunction("MAP(ARRAY [1.0, 2.1], ARRAY [null, null]) != MAP(ARRAY [1.0, 2.1], ARRAY [null, null])", BOOLEAN, null);
    }

    @Test
    public void testDistinctFrom()
    {
        assertFunction("CAST(NULL AS MAP<INTEGER, VARCHAR>) IS DISTINCT FROM CAST(NULL AS MAP<INTEGER, VARCHAR>)", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1], ARRAY[2]) IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM MAP(ARRAY[1], ARRAY[2])", BOOLEAN, true);

        assertFunction("MAP(ARRAY[1], ARRAY[2]) IS DISTINCT FROM MAP(ARRAY[1], ARRAY[2])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1], ARRAY[NULL]) IS DISTINCT FROM MAP(ARRAY[1], ARRAY[NULL])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1], ARRAY[0]) IS DISTINCT FROM MAP(ARRAY[1], ARRAY[NULL])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1], ARRAY[NULL]) IS DISTINCT FROM MAP(ARRAY[1], ARRAY[0])", BOOLEAN, true);

        assertFunction("MAP(ARRAY[1, 2], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['puppies', 'kittens'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 2], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['kittens', 'puppies'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['kittens', 'puppies'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['kittens', 'pupp111'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['kittens', NULL])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY[NULL, NULL])", BOOLEAN, true);

        assertFunction("MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])]) " +
                "IS DISTINCT FROM MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])]) " +
                "IS DISTINCT FROM MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[4e0])])", BOOLEAN, true);
    }

    @Test
    public void testMapConcat()
    {
        assertFunction("MAP_CONCAT(MAP (ARRAY [TRUE], ARRAY [1]), MAP (CAST(ARRAY [] AS ARRAY(BOOLEAN)), CAST(ARRAY [] AS ARRAY(INTEGER))))", mapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 1));
        // <BOOLEAN, INTEGER> Tests
        assertFunction("MAP_CONCAT(MAP (ARRAY [TRUE], ARRAY [1]), MAP (ARRAY [TRUE, FALSE], ARRAY [10, 20]))", mapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 10, false, 20));
        assertFunction("MAP_CONCAT(MAP (ARRAY [TRUE, FALSE], ARRAY [1, 2]), MAP (ARRAY [TRUE, FALSE], ARRAY [10, 20]))", mapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 10, false, 20));
        assertFunction("MAP_CONCAT(MAP (ARRAY [TRUE, FALSE], ARRAY [1, 2]), MAP (ARRAY [TRUE], ARRAY [10]))", mapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 10, false, 2));

        // <VARCHAR, INTEGER> Tests
        assertFunction("MAP_CONCAT(MAP (ARRAY ['1', '2', '3'], ARRAY [1, 2, 3]), MAP (ARRAY ['1', '2', '3', '4'], ARRAY [10, 20, 30, 40]))", mapType(createVarcharType(1), INTEGER), ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 40));
        assertFunction("MAP_CONCAT(MAP (ARRAY ['1', '2', '3', '4'], ARRAY [1, 2, 3, 4]), MAP (ARRAY ['1', '2', '3', '4'], ARRAY [10, 20, 30, 40]))", mapType(createVarcharType(1), INTEGER), ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 40));
        assertFunction("MAP_CONCAT(MAP (ARRAY ['1', '2', '3', '4'], ARRAY [1, 2, 3, 4]), MAP (ARRAY ['1', '2', '3'], ARRAY [10, 20, 30]))", mapType(createVarcharType(1), INTEGER), ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 4));

        // <BIGINT, ARRAY<DOUBLE>> Tests
        assertFunction("MAP_CONCAT(MAP (ARRAY [1, 2, 3], ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0]]), MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [10.0E0], ARRAY [20.0E0], ARRAY [30.0E0], ARRAY [40.0E0]]))", mapType(INTEGER, new ArrayType(DOUBLE)), ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(40.0)));
        assertFunction("MAP_CONCAT(MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]]), MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [10.0E0], ARRAY [20.0E0], ARRAY [30.0E0], ARRAY [40.0E0]]))", mapType(INTEGER, new ArrayType(DOUBLE)), ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(40.0)));
        assertFunction("MAP_CONCAT(MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]]), MAP (ARRAY [1, 2, 3], ARRAY [ARRAY [10.0E0], ARRAY [20.0E0], ARRAY [30.0E0]]))", mapType(INTEGER, new ArrayType(DOUBLE)), ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(4.0)));

        // <ARRAY<DOUBLE>, VARCHAR> Tests
        assertFunction(
                "MAP_CONCAT(MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0]], ARRAY ['1', '2', '3']), MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]], ARRAY ['10', '20', '30', '40']))",
                mapType(new ArrayType(DOUBLE), createVarcharType(2)),
                ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "40"));
        assertFunction(
                "MAP_CONCAT(MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0]], ARRAY ['1', '2', '3']), MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]], ARRAY ['10', '20', '30', '40']))",
                mapType(new ArrayType(DOUBLE), createVarcharType(2)),
                ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "40"));
        assertFunction("MAP_CONCAT(MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]], ARRAY ['1', '2', '3', '4']), MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0]], ARRAY ['10', '20', '30']))",
                mapType(new ArrayType(DOUBLE), createVarcharType(2)),
                ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "4"));

        // Tests for concatenating multiple maps
        assertFunction("MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), NULL, MAP(ARRAY[3], ARRAY[-3]))", mapType(INTEGER, INTEGER), null);
        assertFunction("MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), MAP(ARRAY[2], ARRAY[-2]), MAP(ARRAY[3], ARRAY[-3]))", mapType(INTEGER, INTEGER), ImmutableMap.of(1, -1, 2, -2, 3, -3));
        assertFunction("MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), MAP(ARRAY[1], ARRAY[-2]), MAP(ARRAY[1], ARRAY[-3]))", mapType(INTEGER, INTEGER), ImmutableMap.of(1, -3));
        assertFunction("MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), MAP(ARRAY[], ARRAY[]), MAP(ARRAY[3], ARRAY[-3]))", mapType(INTEGER, INTEGER), ImmutableMap.of(1, -1, 3, -3));
        assertFunction("MAP_CONCAT(MAP(ARRAY[], ARRAY[]), MAP(ARRAY['a_string'], ARRAY['b_string']), cast(MAP(ARRAY[], ARRAY[]) AS MAP(VARCHAR, VARCHAR)))", mapType(VARCHAR, VARCHAR), ImmutableMap.of("a_string", "b_string"));
        assertFunction("MAP_CONCAT(MAP(ARRAY[], ARRAY[]), MAP(ARRAY[], ARRAY[]), MAP(ARRAY[], ARRAY[]))", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("MAP_CONCAT(MAP(), MAP(), MAP())", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), MAP(), MAP(ARRAY[3], ARRAY[-3]))", mapType(INTEGER, INTEGER), ImmutableMap.of(1, -1, 3, -3));
        assertFunction("MAP_CONCAT(MAP(ARRAY[TRUE], ARRAY[1]), MAP(ARRAY[TRUE, FALSE], ARRAY[10, 20]), MAP(ARRAY[FALSE], ARRAY[0]))", mapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 10, false, 0));

        assertCachedInstanceHasBoundedRetainedSize("MAP_CONCAT(MAP (ARRAY ['1', '2', '3'], ARRAY [1, 2, 3]), MAP (ARRAY ['1', '2', '3', '4'], ARRAY [10, 20, 30, 40]))");

        // <DECIMAL, DECIMAL>
        assertFunction(
                "MAP_CONCAT(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]), MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.1, 3.2]))",
                mapType(createDecimalType(29, 14), createDecimalType(2, 1)),
                ImmutableMap.of(decimal("000000000000001.00000000000000"), decimal("2.1"), decimal("383838383838383.12324234234234"), decimal("3.2")));
        assertFunction(
                "MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [3.2]))",
                mapType(createDecimalType(2, 1), createDecimalType(2, 1)),
                ImmutableMap.of(decimal("1.0"), decimal("2.2"), decimal("5.1"), decimal("3.2")));

        // Decimal with type only coercion
        assertFunction(
                "MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [33.2]))",
                mapType(createDecimalType(2, 1), createDecimalType(3, 1)),
                ImmutableMap.of(decimal("1.0"), decimal("2.2"), decimal("5.1"), decimal("33.2")));
        assertFunction(
                "MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [55.1], ARRAY [33.2]))",
                mapType(createDecimalType(3, 1), createDecimalType(3, 1)),
                ImmutableMap.of(decimal("01.0"), decimal("2.2"), decimal("55.1"), decimal("33.2")));

        assertFunction(
                "MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [33.22]))",
                mapType(createDecimalType(2, 1), createDecimalType(4, 2)),
                ImmutableMap.of(decimal("5.1"), decimal("33.22"), decimal("1.0"), decimal("2.20")));
        assertFunction(
                "MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [00000000000000002.2]))",
                mapType(createDecimalType(2, 1), createDecimalType(2, 1)),
                ImmutableMap.of(decimal("1.0"), decimal("2.2"), decimal("5.1"), decimal("2.2")));
    }

    @Test
    public void testMapToMapCast()
    {
        assertFunction("CAST(MAP(ARRAY['1', '100'], ARRAY[true, false]) AS MAP<varchar,bigint>)", mapType(VARCHAR, BIGINT), ImmutableMap.of("1", 1L, "100", 0L));
        assertFunction("CAST(MAP(ARRAY[1,2], ARRAY[1,2]) AS MAP<bigint, boolean>)", mapType(BIGINT, BOOLEAN), ImmutableMap.of(1L, true, 2L, true));
        assertFunction("CAST(MAP(ARRAY[1,2], ARRAY[array[1],array[2]]) AS MAP<bigint, array<boolean>>)", mapType(BIGINT, new ArrayType(BOOLEAN)), ImmutableMap.of(1L, ImmutableList.of(true), 2L, ImmutableList.of(true)));
        assertFunction("CAST(MAP(ARRAY[1], ARRAY[MAP(ARRAY[1.0E0], ARRAY[false])]) AS MAP<varchar, MAP(bigint,bigint)>)", mapType(VARCHAR, mapType(BIGINT, BIGINT)), ImmutableMap.of("1", ImmutableMap.of(1L, 0L)));
        assertFunction("CAST(MAP(ARRAY[1,2], ARRAY[DATE '2016-01-02', DATE '2016-02-03']) AS MAP(bigint, varchar))", mapType(BIGINT, VARCHAR), ImmutableMap.of(1L, "2016-01-02", 2L, "2016-02-03"));
        assertFunction("CAST(MAP(ARRAY[1,2], ARRAY[TIMESTAMP '2016-01-02 01:02:03', TIMESTAMP '2016-02-03 03:04:05']) AS MAP(bigint, varchar))", mapType(BIGINT, VARCHAR), ImmutableMap.of(1L, "2016-01-02 01:02:03.000", 2L, "2016-02-03 03:04:05.000"));
        assertFunction("CAST(MAP(ARRAY['123', '456'], ARRAY[1.23456E0, 2.34567E0]) AS MAP(integer, real))", mapType(INTEGER, REAL), ImmutableMap.of(123, 1.23456F, 456, 2.34567F));
        assertFunction("CAST(MAP(ARRAY['123', '456'], ARRAY[1.23456E0, 2.34567E0]) AS MAP(smallint, decimal(6,5)))", mapType(SMALLINT, createDecimalType(6, 5)), ImmutableMap.of((short) 123, SqlDecimal.of("1.23456"), (short) 456, SqlDecimal.of("2.34567")));
        assertFunction("CAST(MAP(ARRAY[json '1'], ARRAY[1]) AS MAP(bigint, bigint))", mapType(BIGINT, BIGINT), ImmutableMap.of(1L, 1L));
        assertFunction("CAST(MAP(ARRAY['1'], ARRAY[json '1']) AS MAP(bigint, bigint))", mapType(BIGINT, BIGINT), ImmutableMap.of(1L, 1L));

        // null values
        Map<Long, Double> expected = new HashMap<>();
        expected.put(0L, 1.0);
        expected.put(1L, null);
        expected.put(2L, null);
        expected.put(3L, 2.0);
        assertFunction("CAST(MAP(ARRAY[0, 1, 2, 3], ARRAY[1,NULL, NULL, 2]) AS MAP<BIGINT, DOUBLE>)", mapType(BIGINT, DOUBLE), expected);

        assertInvalidCast("CAST(MAP(ARRAY[1, 2], ARRAY[6, 9]) AS MAP<boolean, bigint>)", "duplicate keys");
        assertInvalidCast("CAST(MAP(ARRAY[json 'null'], ARRAY[1]) AS MAP<bigint, bigint>)", "map key is null");
    }

    @Test
    public void testMapFromEntries()
    {
        assertFunction("map_from_entries(null)", mapType(UNKNOWN, UNKNOWN), null);
        assertFunction("map_from_entries(ARRAY[])", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("map_from_entries(CAST(ARRAY[] AS ARRAY(ROW(DOUBLE, BIGINT))))", mapType(DOUBLE, BIGINT), ImmutableMap.of());
        assertFunction("map_from_entries(ARRAY[(1, 3)])", mapType(INTEGER, INTEGER), ImmutableMap.of(1, 3));
        assertFunction("map_from_entries(ARRAY[(1, 'x'), (2, 'y')])", mapType(INTEGER, createVarcharType(1)), ImmutableMap.of(1, "x", 2, "y"));
        assertFunction("map_from_entries(ARRAY[('x', 1.0E0), ('y', 2.0E0)])", mapType(createVarcharType(1), DOUBLE), ImmutableMap.of("x", 1.0, "y", 2.0));

        assertFunction(
                "map_from_entries(ARRAY[('x', ARRAY[1, 2]), ('y', ARRAY[3, 4])])",
                mapType(createVarcharType(1), new ArrayType(INTEGER)),
                ImmutableMap.of("x", ImmutableList.of(1, 2), "y", ImmutableList.of(3, 4)));
        assertFunction(
                "map_from_entries(ARRAY[(ARRAY[1, 2], 'x'), (ARRAY[3, 4], 'y')])",
                mapType(new ArrayType(INTEGER), createVarcharType(1)),
                ImmutableMap.of(ImmutableList.of(1, 2), "x", ImmutableList.of(3, 4), "y"));
        assertFunction(
                "map_from_entries(ARRAY[('x', MAP(ARRAY[1], ARRAY[2])), ('y', MAP(ARRAY[3], ARRAY[4]))])",
                mapType(createVarcharType(1), mapType(INTEGER, INTEGER)),
                ImmutableMap.of("x", ImmutableMap.of(1, 2), "y", ImmutableMap.of(3, 4)));
        assertFunction(
                "map_from_entries(ARRAY[(MAP(ARRAY[1], ARRAY[2]), 'x'), (MAP(ARRAY[3], ARRAY[4]), 'y')])",
                mapType(mapType(INTEGER, INTEGER), createVarcharType(1)),
                ImmutableMap.of(ImmutableMap.of(1, 2), "x", ImmutableMap.of(3, 4), "y"));

        // null values
        Map<String, Integer> expectedNullValueMap = new HashMap<>();
        expectedNullValueMap.put("x", null);
        expectedNullValueMap.put("y", null);
        assertFunction("map_from_entries(ARRAY[('x', null), ('y', null)])", mapType(createVarcharType(1), UNKNOWN), expectedNullValueMap);

        // invalid invocation
        assertInvalidFunction("map_from_entries(ARRAY[('a', 1), ('a', 2)])", "Duplicate keys (a) are not allowed");
        assertInvalidFunction("map_from_entries(ARRAY[(1, 1), (1, 2)])", "Duplicate keys (1) are not allowed");
        assertInvalidFunction("map_from_entries(ARRAY[(1.0, 1), (1.0, 2)])", "Duplicate keys (1.0) are not allowed");
        assertInvalidFunction("map_from_entries(ARRAY[(ARRAY[1, 2], 1), (ARRAY[1, 2], 2)])", "Duplicate keys ([1, 2]) are not allowed");
        assertInvalidFunction("map_from_entries(ARRAY[(MAP(ARRAY[1], ARRAY[2]), 1), (MAP(ARRAY[1], ARRAY[2]), 2)])", "Duplicate keys ({1=2}) are not allowed");
        assertInvalidFunction("map_from_entries(ARRAY[(null, 1), (null, 2)])", "map key cannot be null");
        assertInvalidFunction("map_from_entries(ARRAY[null])", "map entry cannot be null");
        assertInvalidFunction("map_from_entries(ARRAY[(1, 2), null])", "map entry cannot be null");

        assertCachedInstanceHasBoundedRetainedSize("map_from_entries(ARRAY[('a', 1.0), ('b', 2.0), ('c', 3.0), ('d', 4.0), ('e', 5.0), ('f', 6.0)])");
    }

    @Test
    public void testMultimapFromEntries()
    {
        assertFunction("multimap_from_entries(null)", mapType(UNKNOWN, new ArrayType(UNKNOWN)), null);
        assertFunction("multimap_from_entries(ARRAY[])", mapType(UNKNOWN, new ArrayType(UNKNOWN)), ImmutableMap.of());
        assertFunction("multimap_from_entries(CAST(ARRAY[] AS ARRAY(ROW(DOUBLE, BIGINT))))", mapType(DOUBLE, new ArrayType(BIGINT)), ImmutableMap.of());

        assertFunction(
                "multimap_from_entries(ARRAY[(1, 3), (2, 4), (1, 6), (1, 8), (2, 10)])",
                mapType(INTEGER, new ArrayType(INTEGER)),
                ImmutableMap.of(
                        1, ImmutableList.of(3, 6, 8),
                        2, ImmutableList.of(4, 10)));
        assertFunction(
                "multimap_from_entries(ARRAY[(1, 'x'), (2, 'y'), (1, 'a'), (3, 'b'), (2, 'c'), (3, null)])",
                mapType(INTEGER, new ArrayType(createVarcharType(1))),
                ImmutableMap.of(
                        1, ImmutableList.of("x", "a"),
                        2, ImmutableList.of("y", "c"),
                        3, asList("b", null)));
        assertFunction(
                "multimap_from_entries(ARRAY[('x', 1.0E0), ('y', 2.0E0), ('z', null), ('x', 1.5E0), ('y', 2.5E0)])",
                mapType(createVarcharType(1), new ArrayType(DOUBLE)),
                ImmutableMap.of(
                        "x", ImmutableList.of(1.0, 1.5),
                        "y", ImmutableList.of(2.0, 2.5),
                        "z", singletonList(null)));

        // invalid invocation
        assertInvalidFunction("multimap_from_entries(ARRAY[(null, 1), (null, 2)])", "map key cannot be null");
        assertInvalidFunction("multimap_from_entries(ARRAY[null])", "map entry cannot be null");
        assertInvalidFunction("multimap_from_entries(ARRAY[(1, 2), null])", "map entry cannot be null");

        assertCachedInstanceHasBoundedRetainedSize("multimap_from_entries(ARRAY[('a', 1.0), ('b', 2.0), ('a', 3.0), ('c', 4.0), ('b', 5.0), ('c', 6.0)])");
    }

    @Test
    public void testMapEntries()
    {
        Type unknownEntryType = entryType(UNKNOWN, UNKNOWN);

        assertFunction("map_entries(null)", unknownEntryType, null);
        assertFunction("map_entries(MAP(ARRAY[], null))", unknownEntryType, null);
        assertFunction("map_entries(MAP(null, ARRAY[]))", unknownEntryType, null);
        assertFunction("map_entries(MAP(ARRAY[1, 2, 3], null))", entryType(INTEGER, UNKNOWN), null);
        assertFunction("map_entries(MAP(null, ARRAY[1, 2, 3]))", entryType(UNKNOWN, INTEGER), null);
        assertFunction("map_entries(MAP(ARRAY[], ARRAY[]))", unknownEntryType, ImmutableList.of());
        assertFunction("map_entries(MAP(ARRAY[1], ARRAY['x']))", entryType(INTEGER, createVarcharType(1)), ImmutableList.of(ImmutableList.of(1, "x")));
        assertFunction("map_entries(MAP(ARRAY[1, 2], ARRAY['x', 'y']))", entryType(INTEGER, createVarcharType(1)), ImmutableList.of(ImmutableList.of(1, "x"), ImmutableList.of(2, "y")));

        assertFunction(
                "map_entries(MAP(ARRAY['x', 'y'], ARRAY[ARRAY[1, 2], ARRAY[3, 4]]))",
                entryType(createVarcharType(1), new ArrayType(INTEGER)),
                ImmutableList.of(ImmutableList.of("x", ImmutableList.of(1, 2)), ImmutableList.of("y", ImmutableList.of(3, 4))));
        assertFunction(
                "map_entries(MAP(ARRAY[ARRAY[1.0E0, 2.0E0], ARRAY[3.0E0, 4.0E0]], ARRAY[5.0E0, 6.0E0]))",
                entryType(new ArrayType(DOUBLE), DOUBLE),
                ImmutableList.of(ImmutableList.of(ImmutableList.of(1.0, 2.0), 5.0), ImmutableList.of(ImmutableList.of(3.0, 4.0), 6.0)));
        assertFunction(
                "map_entries(MAP(ARRAY['x', 'y'], ARRAY[MAP(ARRAY[1], ARRAY[2]), MAP(ARRAY[3], ARRAY[4])]))",
                entryType(createVarcharType(1), mapType(INTEGER, INTEGER)),
                ImmutableList.of(ImmutableList.of("x", ImmutableMap.of(1, 2)), ImmutableList.of("y", ImmutableMap.of(3, 4))));
        assertFunction(
                "map_entries(MAP(ARRAY[MAP(ARRAY[1], ARRAY[2]), MAP(ARRAY[3], ARRAY[4])], ARRAY['x', 'y']))",
                entryType(mapType(INTEGER, INTEGER), createVarcharType(1)),
                ImmutableList.of(ImmutableList.of(ImmutableMap.of(1, 2), "x"), ImmutableList.of(ImmutableMap.of(3, 4), "y")));

        // null values
        List<Object> expectedEntries = ImmutableList.of(asList("x", null), asList("y", null));
        assertFunction("map_entries(MAP(ARRAY['x', 'y'], ARRAY[null, null]))", entryType(createVarcharType(1), UNKNOWN), expectedEntries);

        assertCachedInstanceHasBoundedRetainedSize("map_entries(MAP(ARRAY[1, 2], ARRAY['x', 'y']))");
    }

    @Test
    public void testEntryMappings()
    {
        assertFunction(
                "map_from_entries(map_entries(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z'])))",
                mapType(INTEGER, createVarcharType(1)),
                ImmutableMap.of(1, "x", 2, "y", 3, "z"));
        assertFunction(
                "map_entries(map_from_entries(ARRAY[(1, 'x'), (2, 'y'), (3, 'z')]))",
                entryType(INTEGER, createVarcharType(1)),
                ImmutableList.of(ImmutableList.of(1, "x"), ImmutableList.of(2, "y"), ImmutableList.of(3, "z")));
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as map(bigint, bigint))", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[1,2], array[3,4])", BOOLEAN, false);
        assertOperator(INDETERMINATE, "map(array[1,2], array[1.0,2.0])", BOOLEAN, false);
        assertOperator(INDETERMINATE, "map(array[1,2], array[null, 3])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[1,2], array[null, 3.0])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[1,2], array[array[11], array[22]])", BOOLEAN, false);
        assertOperator(INDETERMINATE, "map(array[1,2], array[array[11], array[null]])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[1,2], array[array[11], array[22,null]])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[1,2], array[array[11, null], array[22,null]])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[1,2], array[array[null], array[null]])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[array[1], array[2]], array[array[11], array[22]])", BOOLEAN, false);
        assertOperator(INDETERMINATE, "map(array[row(1), row(2)], array[array[11], array[22]])", BOOLEAN, false);
        assertOperator(INDETERMINATE, "map(array[row(1), row(2)], array[array[11], array[22, null]])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[1E0, 2E0], array[11E0, null])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array[1E0, 2E0], array[11E0, 12E0])", BOOLEAN, false);
        assertOperator(INDETERMINATE, "map(array['a', 'b'], array['c', null])", BOOLEAN, true);
        assertOperator(INDETERMINATE, "map(array['a', 'b'], array['c', 'd'])", BOOLEAN, false);
        assertOperator(INDETERMINATE, "map(array[true,false], array[false,true])", BOOLEAN, false);
        assertOperator(INDETERMINATE, "map(array[true,false], array[false,null])", BOOLEAN, true);
    }

    @Test
    public void testMapHashOperator()
    {
        assertMapHashOperator("MAP(ARRAY[1], ARRAY[2])", INTEGER, INTEGER, ImmutableList.of(1, 2));
        assertMapHashOperator("MAP(ARRAY[1, 2147483647], ARRAY[2147483647, 2])", INTEGER, INTEGER, ImmutableList.of(1, 2147483647, 2147483647, 2));
        assertMapHashOperator("MAP(ARRAY[8589934592], ARRAY[2])", BIGINT, INTEGER, ImmutableList.of(8589934592L, 2));
        assertMapHashOperator("MAP(ARRAY[true], ARRAY[false])", BOOLEAN, BOOLEAN, ImmutableList.of(true, false));
        assertMapHashOperator("MAP(ARRAY['123'], ARRAY['456'])", VARCHAR, VARCHAR, ImmutableList.of(utf8Slice("123"), utf8Slice("456")));
    }

    private void assertMapHashOperator(String inputString, Type keyType, Type valueType, List<Object> elements)
    {
        checkArgument(elements.size() % 2 == 0, "the size of elements should be even number");
        MapType mapType = mapType(keyType, valueType);
        BlockBuilder mapArrayBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder singleMapWriter = mapArrayBuilder.beginBlockEntry();
        for (int i = 0; i < elements.size(); i += 2) {
            appendToBlockBuilder(keyType, elements.get(i), singleMapWriter);
            appendToBlockBuilder(valueType, elements.get(i + 1), singleMapWriter);
        }
        mapArrayBuilder.closeEntry();
        long hashResult = mapType.hash(mapArrayBuilder.build(), 0);

        assertOperator(HASH_CODE, inputString, BIGINT, hashResult);
    }

    private static Type entryType(Type keyType, Type valueType)
    {
        return new ArrayType(RowType.anonymous(ImmutableList.of(keyType, valueType)));
    }
}
