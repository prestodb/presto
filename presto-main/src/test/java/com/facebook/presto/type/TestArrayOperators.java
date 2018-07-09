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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockSerdeUtil.writeBlock;
import static com.facebook.presto.operator.aggregation.TypedSet.MAX_FUNCTION_MEMORY;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.StructuralTestUtil.appendToBlockBuilder;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestArrayOperators
        extends AbstractTestFunctions
{
    public TestArrayOperators() {}

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
    public void testStackRepresentation()
    {
        Block actualBlock = arrayBlockOf(new ArrayType(BIGINT), arrayBlockOf(BIGINT, 1L, 2L), arrayBlockOf(BIGINT, 3L));
        DynamicSliceOutput actualSliceOutput = new DynamicSliceOutput(100);
        writeBlock(functionAssertions.getMetadata().getBlockEncodingSerde(), actualSliceOutput, actualBlock);

        Block expectedBlock = new ArrayType(BIGINT)
                .createBlockBuilder(null, 3)
                .appendStructure(BIGINT.createBlockBuilder(null, 2).writeLong(1).closeEntry().writeLong(2).closeEntry().build())
                .appendStructure(BIGINT.createBlockBuilder(null, 1).writeLong(3).closeEntry().build())
                .build();
        DynamicSliceOutput expectedSliceOutput = new DynamicSliceOutput(100);
        writeBlock(functionAssertions.getMetadata().getBlockEncodingSerde(), expectedSliceOutput, expectedBlock);

        assertEquals(actualSliceOutput.slice(), expectedSliceOutput.slice());
    }

    @Test
    public void testTypeConstructor()
    {
        assertFunction("ARRAY[7]", new ArrayType(INTEGER), ImmutableList.of(7));
        assertFunction("ARRAY[12.34E0, 56.78E0]", new ArrayType(DOUBLE), ImmutableList.of(12.34, 56.78));
    }

    @Test
    public void testArrayElements()
    {
        assertFunction("CAST(ARRAY [null] AS ARRAY<INTEGER>)", new ArrayType(INTEGER), asList((Integer) null));
        assertFunction("CAST(ARRAY [1, 2, 3] AS ARRAY<INTEGER>)", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("CAST(ARRAY [1, null, 3] AS ARRAY<INTEGER>)", new ArrayType(INTEGER), asList(1, null, 3));

        assertFunction("CAST(ARRAY [null] AS ARRAY<BIGINT>)", new ArrayType(BIGINT), asList((Long) null));
        assertFunction("CAST(ARRAY [1, 2, 3] AS ARRAY<BIGINT>)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L));
        assertFunction("CAST(ARRAY [1, null, 3] AS ARRAY<BIGINT>)", new ArrayType(BIGINT), asList(1L, null, 3L));

        assertFunction("CAST(ARRAY [1, 2, 3] AS ARRAY<DOUBLE>)", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.0));
        assertFunction("CAST(ARRAY [1, null, 3] AS ARRAY<DOUBLE>)", new ArrayType(DOUBLE), asList(1.0, null, 3.0));

        assertFunction("CAST(ARRAY ['1', '2'] AS ARRAY<VARCHAR>)", new ArrayType(VARCHAR), ImmutableList.of("1", "2"));
        assertFunction("CAST(ARRAY ['1', '2'] AS ARRAY<DOUBLE>)", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0));

        assertFunction("CAST(ARRAY [true, false] AS ARRAY<BOOLEAN>)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("CAST(ARRAY [true, false] AS ARRAY<VARCHAR>)", new ArrayType(VARCHAR), ImmutableList.of("true", "false"));
        assertFunction("CAST(ARRAY [1, 0] AS ARRAY<BOOLEAN>)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));

        assertFunction("CAST(ARRAY [ARRAY[1], ARRAY[2, 3]] AS ARRAY<ARRAY<DOUBLE>>)", new ArrayType(new ArrayType(DOUBLE)), asList(asList(1.0), asList(2.0, 3.0)));

        assertFunction("CAST(ARRAY [ARRAY[1.0], ARRAY[2.0, 3.0]] AS ARRAY<ARRAY<DOUBLE>>)", new ArrayType(new ArrayType(DOUBLE)), asList(asList(1.0), asList(2.0, 3.0)));
        assertFunction("CAST(ARRAY [ARRAY[1.0E0], ARRAY[2.0E0, 3.0E0]] AS ARRAY<ARRAY<DECIMAL(2,1)>>)",
                new ArrayType(new ArrayType(createDecimalType(2, 1))), asList(asList(decimal("1.0")), asList(decimal("2.0"), decimal("3.0"))));
        assertFunction("CAST(ARRAY [ARRAY[1.0E0], ARRAY[2.0E0, 3.0E0]] AS ARRAY<ARRAY<DECIMAL(20,10)>>)",
                new ArrayType(new ArrayType(createDecimalType(20, 10))),
                asList(asList(decimal("0000000001.0000000000")), asList(decimal("0000000002.0000000000"), decimal("0000000003.0000000000"))));

        assertInvalidFunction("CAST(ARRAY [1, null, 3] AS ARRAY<TIMESTAMP>)", TYPE_MISMATCH);
        assertInvalidFunction("CAST(ARRAY [1, null, 3] AS ARRAY<ARRAY<TIMESTAMP>>)", TYPE_MISMATCH);
        assertInvalidFunction("CAST(ARRAY ['puppies', 'kittens'] AS ARRAY<BIGINT>)", INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testArraySize()
    {
        int size = toIntExact(MAX_FUNCTION_MEMORY.toBytes() + 1);
        assertInvalidFunction(
                "array_distinct(ARRAY['" +
                        Strings.repeat("x", size) + "', '" +
                        Strings.repeat("y", size) + "', '" +
                        Strings.repeat("z", size) +
                        "'])",
                EXCEEDED_FUNCTION_MEMORY_LIMIT);
    }

    @Test
    public void testArrayToJson()
    {
        assertFunction("cast(cast (null as ARRAY<BIGINT>) AS JSON)", JSON, null);
        assertFunction("cast(ARRAY[] AS JSON)", JSON, "[]");
        assertFunction("cast(ARRAY[null, null] AS JSON)", JSON, "[null,null]");

        assertFunction("cast(ARRAY[true, false, null] AS JSON)", JSON, "[true,false,null]");

        assertFunction("cast(cast(ARRAY[1, 2, null] AS ARRAY<TINYINT>) AS JSON)", JSON, "[1,2,null]");
        assertFunction("cast(cast(ARRAY[12345, -12345, null] AS ARRAY<SMALLINT>) AS JSON)", JSON, "[12345,-12345,null]");
        assertFunction("cast(cast(ARRAY[123456789, -123456789, null] AS ARRAY<INTEGER>) AS JSON)", JSON, "[123456789,-123456789,null]");
        assertFunction("cast(cast(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY<BIGINT>) AS JSON)", JSON, "[1234567890123456789,-1234567890123456789,null]");

        assertFunction("CAST(CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY<REAL>) AS JSON)", JSON, "[3.14,\"NaN\",\"Infinity\",\"-Infinity\",null]");
        assertFunction("CAST(ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null] AS JSON)", JSON, "[3.14,1.0E-323,1.0E308,\"NaN\",\"Infinity\",\"-Infinity\",null]");
        assertFunction("CAST(ARRAY[DECIMAL '3.14', null] AS JSON)", JSON, "[3.14,null]");
        assertFunction("CAST(ARRAY[DECIMAL '12345678901234567890.123456789012345678', null] AS JSON)", JSON, "[12345678901234567890.123456789012345678,null]");

        assertFunction("cast(ARRAY['a', 'bb', null] AS JSON)", JSON, "[\"a\",\"bb\",null]");
        assertFunction(
                "cast(ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null] AS JSON)",
                JSON,
                "[123,3.14,false,\"abc\",[1,\"a\",null],{\"a\":1,\"b\":\"str\",\"c\":null},null,null]");

        assertFunction(
                "CAST(ARRAY[TIMESTAMP '1970-01-01 00:00:01', null] AS JSON)",
                JSON,
                format("[\"%s\",null]", sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction(
                "CAST(ARRAY[DATE '2001-08-22', DATE '2001-08-23', null] AS JSON)",
                JSON,
                "[\"2001-08-22\",\"2001-08-23\",null]");

        assertFunction(
                "cast(ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null] AS JSON)",
                JSON,
                "[[1,2],[3,null],[],[null,null],null]");
        assertFunction(
                "cast(ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null] AS JSON)",
                JSON,
                "[{\"a\":1,\"b\":2},{\"none\":null,\"three\":3},{},{\"h1\":null,\"h2\":null},null]");
        assertFunction(
                "cast(ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null] AS JSON)",
                JSON,
                "[[1,2],[3,null],[null,null],null]");
        assertFunction("CAST(ARRAY [12345.12345, 12345.12345, 3.0] AS JSON)", JSON, "[12345.12345,12345.12345,3.00000]");
        assertFunction(
                "CAST(ARRAY [123456789012345678901234567890.87654321, 123456789012345678901234567890.12345678] AS JSON)",
                JSON,
                "[123456789012345678901234567890.87654321,123456789012345678901234567890.12345678]");
    }

    @Test
    public void testJsonToArray()
    {
        // special values
        assertFunction("CAST(CAST (null AS JSON) AS ARRAY<BIGINT>)", new ArrayType(BIGINT), null);
        assertFunction("CAST(JSON 'null' AS ARRAY<BIGINT>)", new ArrayType(BIGINT), null);
        assertFunction("CAST(JSON '[]' AS ARRAY<BIGINT>)", new ArrayType(BIGINT), ImmutableList.of());
        assertFunction("CAST(JSON '[null, null]' AS ARRAY<BIGINT>)", new ArrayType(BIGINT), Lists.newArrayList(null, null));

        // boolean
        assertFunction("CAST(JSON '[true, false, 12, 0, 12.3, 0.0, \"true\", \"false\", null]' AS ARRAY<BOOLEAN>)",
                new ArrayType(BOOLEAN),
                asList(true, false, true, false, true, false, true, false, null));

        // tinyint, smallint, integer, bigint
        assertFunction("CAST(JSON '[true, false, 12, 12.7, \"12\", null]' AS ARRAY<TINYINT>)",
                new ArrayType(TINYINT),
                asList((byte) 1, (byte) 0, (byte) 12, (byte) 13, (byte) 12, null));
        assertFunction("CAST(JSON '[true, false, 12345, 12345.6, \"12345\", null]' AS ARRAY<SMALLINT>)",
                new ArrayType(SMALLINT),
                asList((short) 1, (short) 0, (short) 12345, (short) 12346, (short) 12345, null));
        assertFunction("CAST(JSON '[true, false, 12345678, 12345678.9, \"12345678\", null]' AS ARRAY<INTEGER>)",
                new ArrayType(INTEGER),
                asList(1, 0, 12345678, 12345679, 12345678, null));
        assertFunction("CAST(JSON '[true, false, 1234567891234567, 1234567891234567.8, \"1234567891234567\", null]' AS ARRAY<BIGINT>)",
                new ArrayType(BIGINT),
                asList(1L, 0L, 1234567891234567L, 1234567891234568L, 1234567891234567L, null));

        // real, double, decimal
        assertFunction("CAST(JSON '[true, false, 12345, 12345.67, \"3.14\", \"NaN\", \"Infinity\", \"-Infinity\", null]' AS ARRAY<REAL>)",
                new ArrayType(REAL),
                asList(1.0f, 0.0f, 12345.0f, 12345.67f, 3.14f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, null));
        assertFunction("CAST(JSON '[true, false, 1234567890, 1234567890.1, \"3.14\", \"NaN\", \"Infinity\", \"-Infinity\", null]' AS ARRAY<DOUBLE>)",
                new ArrayType(DOUBLE),
                asList(1.0, 0.0, 1234567890.0, 1234567890.1, 3.14, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null));
        assertFunction("CAST(JSON '[true, false, 128, 123.456, \"3.14\", null]' AS ARRAY<DECIMAL(10, 5)>)",
                new ArrayType(createDecimalType(10, 5)),
                asList(decimal("1.00000"), decimal("0.00000"), decimal("128.00000"), decimal("123.45600"), decimal("3.14000"), null));
        assertFunction("CAST(JSON '[true, false, 128, 12345678.12345678, \"3.14\", null]' AS ARRAY<DECIMAL(38, 8)>)",
                new ArrayType(createDecimalType(38, 8)),
                asList(decimal("1.00000000"), decimal("0.00000000"), decimal("128.00000000"), decimal("12345678.12345678"), decimal("3.14000000"), null));

        // varchar, json
        assertFunction("CAST(JSON '[true, false, 12, 12.3, \"puppies\", \"kittens\", \"null\", \"\", null]' AS ARRAY<VARCHAR>)",
                new ArrayType(VARCHAR),
                asList("true", "false", "12", "12.3", "puppies", "kittens", "null", "", null));
        assertFunction("CAST(JSON '[5, 3.14, [1, 2, 3], \"e\", {\"a\": \"b\"}, null, \"null\", [null]]' AS ARRAY<JSON>)",
                new ArrayType(JSON),
                ImmutableList.of("5", "3.14", "[1,2,3]", "\"e\"", "{\"a\":\"b\"}", "null", "\"null\"", "[null]"));

        // nested array/map
        assertFunction("CAST(JSON '[[1, 2], [3, null], [], [null, null], null]' AS ARRAY<ARRAY<BIGINT>>)",
                new ArrayType(new ArrayType(BIGINT)),
                asList(asList(1L, 2L), asList(3L, null), emptyList(), asList(null, null), null));

        assertFunction("CAST(JSON '[" +
                        "{\"a\": 1, \"b\": 2}, " +
                        "{\"none\": null, \"three\": 3}, " +
                        "{}, " +
                        "{\"h1\": null,\"h2\": null}, " +
                        "null]' " +
                        "AS ARRAY<MAP<VARCHAR, BIGINT>>)",
                new ArrayType(mapType(VARCHAR, BIGINT)),
                asList(
                        ImmutableMap.of("a", 1L, "b", 2L),
                        asMap(ImmutableList.of("none", "three"), asList(null, 3L)),
                        ImmutableMap.of(),
                        asMap(ImmutableList.of("h1", "h2"), asList(null, null)),
                        null));

        assertFunction("CAST(JSON '[" +
                        "[1, \"two\"], " +
                        "[3, null], " +
                        "{\"k1\": 1, \"k2\": \"two\"}, " +
                        "{\"k2\": null, \"k1\": 3}, " +
                        "null]' " +
                        "AS ARRAY<ROW(k1 BIGINT, k2 VARCHAR)>)",
                new ArrayType(RowType.from(ImmutableList.of(
                        RowType.field("k1", BIGINT),
                        RowType.field("k2", VARCHAR)))),
                asList(
                        asList(1L, "two"),
                        asList(3L, null),
                        asList(1L, "two"),
                        asList(3L, null),
                        null));

        // invalid cast
        assertInvalidCast("CAST(JSON '{\"a\": 1}' AS ARRAY<BIGINT>)", "Cannot cast to array(bigint). Expected a json array, but got {\n{\"a\":1}");
        assertInvalidCast("CAST(JSON '[1, 2, 3]' AS ARRAY<ARRAY<BIGINT>>)", "Cannot cast to array(array(bigint)). Expected a json array, but got 1\n[1,2,3]");
        assertInvalidCast("CAST(JSON '[1, {}]' AS ARRAY<BIGINT>)", "Cannot cast to array(bigint). Unexpected token when cast to bigint: {\n[1,{}]");
        assertInvalidCast("CAST(JSON '[[1], {}]' AS ARRAY<ARRAY<BIGINT>>)", "Cannot cast to array(array(bigint)). Expected a json array, but got {\n[[1],{}]");

        assertInvalidCast("CAST(unchecked_to_json('1, 2, 3') AS ARRAY<BIGINT>)", "Cannot cast to array(bigint).\n1, 2, 3");
        assertInvalidCast("CAST(unchecked_to_json('[1] 2') AS ARRAY<BIGINT>)", "Cannot cast to array(bigint). Unexpected trailing token: 2\n[1] 2");
        assertInvalidCast("CAST(unchecked_to_json('[1, 2, 3') AS ARRAY<BIGINT>)", "Cannot cast to array(bigint).\n[1, 2, 3");

        assertInvalidCast("CAST(JSON '[\"a\", \"b\"]' AS ARRAY<BIGINT>)", "Cannot cast to array(bigint). Cannot cast 'a' to BIGINT\n[\"a\",\"b\"]");
        assertInvalidCast("CAST(JSON '[1234567890123.456]' AS ARRAY<INTEGER>)", "Cannot cast to array(integer). Out of range for integer: 1.234567890123456E12\n[1.234567890123456E12]");

        assertFunction("CAST(JSON '[1, 2.0, 3]' AS ARRAY(DECIMAL(10,5)))", new ArrayType(createDecimalType(10, 5)), ImmutableList.of(decimal("1.00000"), decimal("2.00000"), decimal("3.00000")));
        assertFunction("CAST(CAST(ARRAY [1, 2.0, 3] as JSON) AS ARRAY(DECIMAL(10,5)))", new ArrayType(createDecimalType(10, 5)), ImmutableList.of(decimal("1.00000"), decimal("2.00000"), decimal("3.00000")));
        assertFunction(
                "CAST(CAST(ARRAY [123456789012345678901234567890.12345678, 1.2] as JSON) AS ARRAY(DECIMAL(38,8)))",
                new ArrayType(createDecimalType(38, 8)),
                ImmutableList.of(decimal("123456789012345678901234567890.12345678"), decimal("1.20000000")));
        assertFunction(
                "CAST(CAST(ARRAY [12345.87654] as JSON) AS ARRAY(DECIMAL(7,2)))",
                new ArrayType(createDecimalType(7, 2)),
                ImmutableList.of(decimal("12345.88")));
        assertInvalidCast("CAST(CAST(ARRAY [12345.12345] as JSON) AS ARRAY(DECIMAL(6,2)))");
    }

    @Test
    public void testConstructor()
    {
        assertFunction("ARRAY []", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("ARRAY [NULL]", new ArrayType(UNKNOWN), Lists.newArrayList((Object) null));
        assertFunction("ARRAY [1, 2, 3]", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("ARRAY [1, NULL, 3]", new ArrayType(INTEGER), Lists.newArrayList(1, null, 3));
        assertFunction("ARRAY [NULL, 2, 3]", new ArrayType(INTEGER), Lists.newArrayList(null, 2, 3));
        assertFunction("ARRAY [1, 2.0E0, 3]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.0));
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]]", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));
        assertFunction("ARRAY [ARRAY[1, 2], NULL, ARRAY[3]]", new ArrayType(new ArrayType(INTEGER)), Lists.newArrayList(ImmutableList.of(1, 2), null, ImmutableList.of(3)));
        assertFunction("ARRAY [BIGINT '1', 2, 3]", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L));
        assertFunction("ARRAY [1, CAST (NULL AS BIGINT), 3]", new ArrayType(BIGINT), Lists.newArrayList(1L, null, 3L));
        assertFunction("ARRAY [NULL, 20000000000, 30000000000]", new ArrayType(BIGINT), Lists.newArrayList(null, 20000000000L, 30000000000L));
        assertFunction("ARRAY [1, 2.0E0, 3]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.0));
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]]", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));
        assertFunction("ARRAY [ARRAY[1, 2], NULL, ARRAY[3]]", new ArrayType(new ArrayType(INTEGER)), Lists.newArrayList(ImmutableList.of(1, 2), null, ImmutableList.of(3)));
        assertFunction("ARRAY [ARRAY[1, 2], NULL, ARRAY[BIGINT '3']]", new ArrayType(new ArrayType(BIGINT)), Lists.newArrayList(ImmutableList.of(1L, 2L), null, ImmutableList.of(3L)));
        assertFunction("ARRAY [1.0E0, 2.5E0, 3.0E0]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.5, 3.0));
        assertFunction("ARRAY [1, 2.5E0, 3]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.5, 3.0));
        assertFunction("ARRAY ['puppies', 'kittens']", new ArrayType(createVarcharType(7)), ImmutableList.of("puppies", "kittens"));
        assertFunction("ARRAY [TRUE, FALSE]", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction(
                "ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction("ARRAY [sqrt(-1)]", new ArrayType(DOUBLE), ImmutableList.of(NaN));
        assertFunction("ARRAY [pow(infinity(), 2)]", new ArrayType(DOUBLE), ImmutableList.of(POSITIVE_INFINITY));
        assertFunction("ARRAY [pow(-infinity(), 1)]", new ArrayType(DOUBLE), ImmutableList.of(NEGATIVE_INFINITY));
        assertFunction("ARRAY [ARRAY [], NULL]", new ArrayType(new ArrayType(UNKNOWN)), asList(ImmutableList.of(), null));
        assertFunction(
                "ARRAY [ARRAY[1.0], ARRAY[2.0, 3.0]]",
                new ArrayType(new ArrayType(createDecimalType(2, 1))),
                asList(asList(decimal("1.0")), asList(decimal("2.0"), decimal("3.0"))));
        assertFunction(
                "ARRAY[1.0, 2.0, 3.11]",
                new ArrayType(createDecimalType(3, 2)),
                asList(decimal("1.00"), decimal("2.00"), decimal("3.11")));
        assertFunction(
                "ARRAY[1, 2.0, 3.11]",
                new ArrayType(createDecimalType(12, 2)),
                asList(decimal("0000000001.00"), decimal("0000000002.00"), decimal("0000000003.11")));
        assertFunction(
                "ARRAY [ARRAY[1.0], ARRAY[2.0, 123456789123456.789]]",
                new ArrayType(new ArrayType(createDecimalType(18, 3))),
                asList(asList(decimal("000000000000001.000")), asList(decimal("000000000000002.000"), decimal("123456789123456.789"))));
    }

    @Test
    public void testArrayToArrayConcat()
    {
        assertFunction("ARRAY [1, NULL] || ARRAY [3]", new ArrayType(INTEGER), Lists.newArrayList(1, null, 3));
        assertFunction("ARRAY [1, 2] || ARRAY[3, 4]", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3, 4));
        assertFunction("ARRAY [1, 2] || ARRAY[3, BIGINT '4']", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 4L));
        assertFunction("ARRAY [1, 2] || ARRAY[3, 40000000000]", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 40000000000L));
        assertFunction("ARRAY [NULL] || ARRAY[NULL]", new ArrayType(UNKNOWN), Lists.newArrayList(null, null));
        assertFunction("ARRAY ['puppies'] || ARRAY ['kittens']", new ArrayType(createVarcharType(7)), ImmutableList.of("puppies", "kittens"));
        assertFunction("ARRAY [TRUE] || ARRAY [FALSE]", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("concat(ARRAY [1] , ARRAY[2,3])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("ARRAY [TIMESTAMP '1970-01-01 00:00:01'] || ARRAY[TIMESTAMP '1973-07-08 22:00:01']", new ArrayType(TIMESTAMP), ImmutableList.of(
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction("ARRAY [ARRAY[ARRAY[1]]] || ARRAY [ARRAY[ARRAY[2]]]",
                new ArrayType(new ArrayType(new ArrayType(INTEGER))),
                asList(singletonList(Ints.asList(1)), singletonList(Ints.asList(2))));
        assertFunction("ARRAY [] || ARRAY []", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("ARRAY [TRUE] || ARRAY [FALSE] || ARRAY [TRUE]", new ArrayType(BOOLEAN), ImmutableList.of(true, false, true));
        assertFunction("ARRAY [1] || ARRAY [2] || ARRAY [3] || ARRAY [4]", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3, 4));
        assertFunction("ARRAY [1] || ARRAY [2.0E0] || ARRAY [3] || ARRAY [4.0E0]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.0, 4.0));
        assertFunction("ARRAY [ARRAY [1], ARRAY [2, 8]] || ARRAY [ARRAY [3, 6], ARRAY [4]]", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 8), ImmutableList.of(3, 6), ImmutableList.of(4)));
        assertFunction(
                "ARRAY[1.0] || ARRAY [2.0, 3.11]",
                new ArrayType(createDecimalType(3, 2)),
                ImmutableList.of(decimal("1.00"), decimal("2.00"), decimal("3.11")));
        assertFunction(
                "ARRAY[1.0] || ARRAY [2.0] || ARRAY [123456789123456.789]",
                new ArrayType(createDecimalType(18, 3)),
                ImmutableList.of(decimal("000000000000001.000"), decimal("000000000000002.000"), decimal("123456789123456.789")));

        // Tests for concatenating multiple arrays
        List<Object> nullList = Collections.nCopies(2, null);
        assertFunction("concat(ARRAY[], ARRAY[NULL], ARRAY[], ARRAY[NULL], ARRAY[])", new ArrayType(UNKNOWN), nullList);
        assertFunction("concat(ARRAY[], ARRAY[], ARRAY[], NULL, ARRAY[])", new ArrayType(UNKNOWN), null);
        assertFunction("concat(ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("concat(ARRAY[], ARRAY[], ARRAY[333], ARRAY[], ARRAY[])", new ArrayType(INTEGER), ImmutableList.of(333));
        assertFunction("concat(ARRAY[1], ARRAY[2,3], ARRAY[])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("concat(ARRAY[1], ARRAY[2,3,3], ARRAY[2,1])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3, 3, 2, 1));
        assertFunction("concat(ARRAY[1], ARRAY[], ARRAY[1,2])", new ArrayType(INTEGER), ImmutableList.of(1, 1, 2));
        assertFunction("concat(ARRAY[], ARRAY[1], ARRAY[], ARRAY[3], ARRAY[], ARRAY[5], ARRAY[])", new ArrayType(INTEGER), ImmutableList.of(1, 3, 5));
        assertFunction("concat(ARRAY[], ARRAY['123456'], CAST(ARRAY[1,2] AS ARRAY(varchar)), ARRAY[])", new ArrayType(VARCHAR), ImmutableList.of("123456", "1", "2"));

        assertInvalidFunction("ARRAY [ARRAY[1]] || ARRAY[ARRAY[true], ARRAY[false]]", FUNCTION_NOT_FOUND);

        // This query is ambiguous. The result can be [[1], NULL] or [[1], [NULL]] depending on interpretation
        assertInvalidFunction("ARRAY [ARRAY [1]] || ARRAY [NULL]", AMBIGUOUS_FUNCTION_CALL);

        try {
            assertFunction("ARRAY [ARRAY [1]] || ARRAY [ARRAY ['x']]", new ArrayType(new ArrayType(INTEGER)), null);
            fail("arrays must be of the same type");
        }
        catch (RuntimeException e) {
            // Expected
        }

        assertCachedInstanceHasBoundedRetainedSize("ARRAY [1, NULL] || ARRAY [3]");
    }

    @Test
    public void testElementArrayConcat()
    {
        assertFunction("CAST (ARRAY [DATE '2001-08-22'] || DATE '2001-08-23' AS JSON)", JSON, "[\"2001-08-22\",\"2001-08-23\"]");
        assertFunction("CAST (DATE '2001-08-23' || ARRAY [DATE '2001-08-22'] AS JSON)", JSON, "[\"2001-08-23\",\"2001-08-22\"]");
        assertFunction("1 || ARRAY [2]", new ArrayType(INTEGER), Lists.newArrayList(1, 2));
        assertFunction("ARRAY [2] || 1", new ArrayType(INTEGER), Lists.newArrayList(2, 1));
        assertFunction("ARRAY [2] || BIGINT '1'", new ArrayType(BIGINT), Lists.newArrayList(2L, 1L));
        assertFunction("TRUE || ARRAY [FALSE]", new ArrayType(BOOLEAN), Lists.newArrayList(true, false));
        assertFunction("ARRAY [FALSE] || TRUE", new ArrayType(BOOLEAN), Lists.newArrayList(false, true));
        assertFunction("1.0E0 || ARRAY [2.0E0]", new ArrayType(DOUBLE), Lists.newArrayList(1.0, 2.0));
        assertFunction("ARRAY [2.0E0] || 1.0E0", new ArrayType(DOUBLE), Lists.newArrayList(2.0, 1.0));
        assertFunction("'puppies' || ARRAY ['kittens']", new ArrayType(createVarcharType(7)), Lists.newArrayList("puppies", "kittens"));
        assertFunction("ARRAY ['kittens'] || 'puppies'", new ArrayType(createVarcharType(7)), Lists.newArrayList("kittens", "puppies"));
        assertFunction("ARRAY [TIMESTAMP '1970-01-01 00:00:01'] || TIMESTAMP '1973-07-08 22:00:01'", new ArrayType(TIMESTAMP), ImmutableList.of(
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction("TIMESTAMP '1973-07-08 22:00:01' || ARRAY [TIMESTAMP '1970-01-01 00:00:01']", new ArrayType(TIMESTAMP), ImmutableList.of(
                sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction("ARRAY [2, 8] || ARRAY[ARRAY[3, 6], ARRAY[4]]", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(2, 8), ImmutableList.of(3, 6), ImmutableList.of(4)));
        assertFunction("ARRAY [ARRAY [1], ARRAY [2, 8]] || ARRAY [3, 6]", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 8), ImmutableList.of(3, 6)));
        assertFunction(
                "ARRAY [2.0, 3.11] || 1.0",
                new ArrayType(createDecimalType(3, 2)),
                asList(decimal("2.00"), decimal("3.11"), decimal("1.00")));
        assertFunction(
                "ARRAY[1.0] || 2.0 || 123456789123456.789",
                new ArrayType(createDecimalType(18, 3)),
                asList(decimal("000000000000001.000"), decimal("000000000000002.000"), decimal("123456789123456.789")));

        try {
            assertFunction("ARRAY [ARRAY[1]] || ARRAY ['x']", new ArrayType(new ArrayType(INTEGER)), null);
            fail("arrays must be of the same type");
        }
        catch (RuntimeException e) {
            // Expected
        }

        assertCachedInstanceHasBoundedRetainedSize("ARRAY [1, NULL] || 3");
        assertCachedInstanceHasBoundedRetainedSize("3 || ARRAY [1, NULL]");
    }

    @Test
    public void testArrayContains()
    {
        assertFunction("CONTAINS(ARRAY ['puppies', 'dogs'], 'dogs')", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [1, 2, 3], 2)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [1, BIGINT '2', 3], 2)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [1, 2, 3], BIGINT '2')", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [1, 2, 3], 5)", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [1, NULL, 3], 1)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [NULL, 2, 3], 1)", BOOLEAN, null);
        assertFunction("CONTAINS(ARRAY [NULL, 2, 3], NULL)", BOOLEAN, null);
        assertFunction("CONTAINS(ARRAY [1, 2.0E0, 3], 3.0E0)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [1.0E0, 2.5E0, 3.0E0], 2.2E0)", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY ['puppies', 'dogs'], 'dogs')", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY ['puppies', 'dogs'], 'sharks')", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [TRUE, FALSE], TRUE)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [FALSE], TRUE)", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [3, 4])", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [3])", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [CAST (NULL AS BIGINT)], 1)", BOOLEAN, null);
        assertFunction("CONTAINS(ARRAY [CAST (NULL AS BIGINT)], NULL)", BOOLEAN, null);
        assertFunction("CONTAINS(ARRAY [], NULL)", BOOLEAN, null);
        assertFunction("CONTAINS(ARRAY [], 1)", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [2.2, 1.1], 1.1)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [2.2, 1.1], 1.1)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [2.2, NULL], 1.1)", BOOLEAN, null);
        assertFunction("CONTAINS(ARRAY [2.2, 1.1], 1.2)", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [2.2, 1.1], 0000000000001.100)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [2.2, 001.20], 1.2)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [ARRAY [1.1, 2.2], ARRAY [3.3, 4.3]], ARRAY [3.3, 4.300])", BOOLEAN, true);
    }

    @Test
    public void testArrayJoin()
    {
        assertFunction("array_join(ARRAY[1, NULL, 2], ',')", VARCHAR, "1,2");
        assertFunction("ARRAY_JOIN(ARRAY [1, 2, 3], ';', 'N/A')", VARCHAR, "1;2;3");
        assertFunction("ARRAY_JOIN(ARRAY [1, 2, null], ';', 'N/A')", VARCHAR, "1;2;N/A");
        assertFunction("ARRAY_JOIN(ARRAY [1, 2, 3], 'x')", VARCHAR, "1x2x3");
        assertFunction("ARRAY_JOIN(ARRAY [BIGINT '1', 2, 3], 'x')", VARCHAR, "1x2x3");
        assertFunction("ARRAY_JOIN(ARRAY [null], '=')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [null,null], '=')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [], 'S')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [''], '', '')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [1, 2, 3, null, 5], ',', '*')", VARCHAR, "1,2,3,*,5");
        assertFunction("ARRAY_JOIN(ARRAY ['a', 'b', 'c', null, null, 'd'], '-', 'N/A')", VARCHAR, "a-b-c-N/A-N/A-d");
        assertFunction("ARRAY_JOIN(ARRAY ['a', 'b', 'c', null, null, 'd'], '-')", VARCHAR, "a-b-c-d");
        assertFunction("ARRAY_JOIN(ARRAY [null, null, null, null], 'X')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [true, false], 'XX')", VARCHAR, "trueXXfalse");
        assertFunction("ARRAY_JOIN(ARRAY [sqrt(-1), infinity()], ',')", VARCHAR, "NaN,Infinity");
        assertFunction("ARRAY_JOIN(ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'], '|')", VARCHAR, format(
                "%s|%s",
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction(
                "ARRAY_JOIN(ARRAY [null, TIMESTAMP '1970-01-01 00:00:01'], '|')",
                VARCHAR,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION).toString());
        assertFunction(
                "ARRAY_JOIN(ARRAY [null, TIMESTAMP '1970-01-01 00:00:01'], '|', 'XYZ')",
                VARCHAR,
                "XYZ|" + sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION).toString());
        assertFunction("ARRAY_JOIN(ARRAY [1.0, 2.1, 3.3], 'x')", VARCHAR, "1.0x2.1x3.3");
        assertFunction("ARRAY_JOIN(ARRAY [1.0, 2.100, 3.3], 'x')", VARCHAR, "1.000x2.100x3.300");
        assertFunction("ARRAY_JOIN(ARRAY [1.0, 2.100, NULL], 'x', 'N/A')", VARCHAR, "1.000x2.100xN/A");
        assertFunction("ARRAY_JOIN(ARRAY [1.0, DOUBLE '002.100', 3.3], 'x')", VARCHAR, "1.0x2.1x3.3");

        assertInvalidFunction("ARRAY_JOIN(ARRAY [ARRAY [1], ARRAY [2]], '-')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("ARRAY_JOIN(ARRAY [MAP(ARRAY [1], ARRAY [2])], '-')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("ARRAY_JOIN(ARRAY [cast(row(1, 2) AS row(col0 bigint, col1 bigint))], '-')", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testCardinality()
    {
        assertFunction("CARDINALITY(ARRAY [])", BIGINT, 0L);
        assertFunction("CARDINALITY(ARRAY [NULL])", BIGINT, 1L);
        assertFunction("CARDINALITY(ARRAY [1, 2, 3])", BIGINT, 3L);
        assertFunction("CARDINALITY(ARRAY [1, NULL, 3])", BIGINT, 3L);
        assertFunction("CARDINALITY(ARRAY [1, 2.0E0, 3])", BIGINT, 3L);
        assertFunction("CARDINALITY(ARRAY [ARRAY[1, 2], ARRAY[3]])", BIGINT, 2L);
        assertFunction("CARDINALITY(ARRAY [1.0E0, 2.5E0, 3.0E0])", BIGINT, 3L);
        assertFunction("CARDINALITY(ARRAY ['puppies', 'kittens'])", BIGINT, 2L);
        assertFunction("CARDINALITY(ARRAY [TRUE, FALSE])", BIGINT, 2L);
        assertFunction("CARDINALITY(ARRAY [1.1, 2.2, 3.3])", BIGINT, 3L);
        assertFunction("CARDINALITY(ARRAY [1.1, 33832293522235.23522])", BIGINT, 2L);
    }

    @Test
    public void testArrayMin()
    {
        assertFunction("ARRAY_MIN(ARRAY [])", UNKNOWN, null);
        assertFunction("ARRAY_MIN(ARRAY [NULL])", UNKNOWN, null);
        assertFunction("ARRAY_MIN(ARRAY [NaN()])", DOUBLE, NaN);
        assertFunction("ARRAY_MIN(ARRAY [NULL, NULL, NULL])", UNKNOWN, null);
        assertFunction("ARRAY_MIN(ARRAY [NaN(), NaN(), NaN()])", DOUBLE, NaN);
        assertFunction("ARRAY_MIN(ARRAY [NULL, 2, 3])", INTEGER, null);
        assertFunction("ARRAY_MIN(ARRAY [NaN(), 2, 3])", DOUBLE, NaN);
        assertFunction("ARRAY_MIN(ARRAY [NULL, NaN(), 1])", DOUBLE, NaN);
        assertFunction("ARRAY_MIN(ARRAY [NaN(), NULL, 3.0])", DOUBLE, NaN);
        assertFunction("ARRAY_MIN(ARRAY [1.0E0, NULL, 3])", DOUBLE, null);
        assertFunction("ARRAY_MIN(ARRAY [1.0, NaN(), 3])", DOUBLE, NaN);
        assertFunction("ARRAY_MIN(ARRAY ['1', '2', NULL])", createVarcharType(1), null);
        assertFunction("ARRAY_MIN(ARRAY [3, 2, 1])", INTEGER, 1);
        assertFunction("ARRAY_MIN(ARRAY [1, 2, 3])", INTEGER, 1);
        assertFunction("ARRAY_MIN(ARRAY [BIGINT '3', 2, 1])", BIGINT, 1L);
        assertFunction("ARRAY_MIN(ARRAY [1, 2.0E0, 3])", DOUBLE, 1.0);
        assertFunction("ARRAY_MIN(ARRAY [ARRAY[1, 2], ARRAY[3]])", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("ARRAY_MIN(ARRAY [1.0E0, 2.5E0, 3.0E0])", DOUBLE, 1.0);
        assertFunction("ARRAY_MIN(ARRAY ['puppies', 'kittens'])", createVarcharType(7), "kittens");
        assertFunction("ARRAY_MIN(ARRAY [TRUE, FALSE])", BOOLEAN, false);
        assertFunction("ARRAY_MIN(ARRAY [NULL, FALSE])", BOOLEAN, null);
        assertDecimalFunction("ARRAY_MIN(ARRAY [2.1, 2.2, 2.3])", decimal("2.1"));
        assertDecimalFunction("ARRAY_MIN(ARRAY [2.111111222111111114111, 2.22222222222222222, 2.222222222222223])", decimal("2.111111222111111114111"));
        assertDecimalFunction("ARRAY_MIN(ARRAY [1.9, 2, 2.3])", decimal("0000000001.9"));
        assertDecimalFunction("ARRAY_MIN(ARRAY [2.22222222222222222, 2.3])", decimal("2.22222222222222222"));
    }

    @Test
    public void testArrayMax()
    {
        assertFunction("ARRAY_MAX(ARRAY [])", UNKNOWN, null);
        assertFunction("ARRAY_MAX(ARRAY [NULL])", UNKNOWN, null);
        assertFunction("ARRAY_MAX(ARRAY [NaN()])", DOUBLE, NaN);
        assertFunction("ARRAY_MAX(ARRAY [NULL, NULL, NULL])", UNKNOWN, null);
        assertFunction("ARRAY_MAX(ARRAY [NaN(), NaN(), NaN()])", DOUBLE, NaN);
        assertFunction("ARRAY_MAX(ARRAY [NULL, 2, 3])", INTEGER, null);
        assertFunction("ARRAY_MAX(ARRAY [NaN(), 2, 3])", DOUBLE, NaN);
        assertFunction("ARRAY_MAX(ARRAY [NULL, NaN(), 1])", DOUBLE, NaN);
        assertFunction("ARRAY_MAX(ARRAY [NaN(), NULL, 3.0])", DOUBLE, NaN);
        assertFunction("ARRAY_MAX(ARRAY [1.0E0, NULL, 3])", DOUBLE, null);
        assertFunction("ARRAY_MAX(ARRAY [1.0, NaN(), 3])", DOUBLE, NaN);
        assertFunction("ARRAY_MAX(ARRAY ['1', '2', NULL])", createVarcharType(1), null);
        assertFunction("ARRAY_MAX(ARRAY [3, 2, 1])", INTEGER, 3);
        assertFunction("ARRAY_MAX(ARRAY [1, 2, 3])", INTEGER, 3);
        assertFunction("ARRAY_MAX(ARRAY [BIGINT '1', 2, 3])", BIGINT, 3L);
        assertFunction("ARRAY_MAX(ARRAY [1, 2.0E0, 3])", DOUBLE, 3.0);
        assertFunction("ARRAY_MAX(ARRAY [ARRAY[1, 2], ARRAY[3]])", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("ARRAY_MAX(ARRAY [1.0E0, 2.5E0, 3.0E0])", DOUBLE, 3.0);
        assertFunction("ARRAY_MAX(ARRAY ['puppies', 'kittens'])", createVarcharType(7), "puppies");
        assertFunction("ARRAY_MAX(ARRAY [TRUE, FALSE])", BOOLEAN, true);
        assertFunction("ARRAY_MAX(ARRAY [NULL, FALSE])", BOOLEAN, null);
        assertDecimalFunction("ARRAY_MAX(ARRAY [2.1, 2.2, 2.3])", decimal("2.3"));
        assertDecimalFunction("ARRAY_MAX(ARRAY [2.111111222111111114111, 2.22222222222222222, 2.222222222222223])", decimal("2.222222222222223000000"));
        assertDecimalFunction("ARRAY_MAX(ARRAY [1.9, 2, 2.3])", decimal("0000000002.3"));
        assertDecimalFunction("ARRAY_MAX(ARRAY [2.22222222222222222, 2.3])", decimal("2.30000000000000000"));
    }

    @Test
    public void testArrayPosition()
    {
        assertFunction("ARRAY_POSITION(ARRAY [10, 20, 30, 40], 30)", BIGINT, 3L);
        assertFunction("ARRAY_POSITION(CAST (JSON '[]' as array(bigint)), 30)", BIGINT, 0L);
        assertFunction("ARRAY_POSITION(ARRAY [cast(NULL as bigint)], 30)", BIGINT, 0L);
        assertFunction("ARRAY_POSITION(ARRAY [cast(NULL as bigint), NULL, NULL], 30)", BIGINT, 0L);
        assertFunction("ARRAY_POSITION(ARRAY [NULL, NULL, 30, NULL], 30)", BIGINT, 3L);

        assertFunction("ARRAY_POSITION(ARRAY [1.1E0, 2.1E0, 3.1E0, 4.1E0], 3.1E0)", BIGINT, 3L);
        assertFunction("ARRAY_POSITION(ARRAY [false, false, true, true], true)", BIGINT, 3L);
        assertFunction("ARRAY_POSITION(ARRAY ['10', '20', '30', '40'], '30')", BIGINT, 3L);

        assertFunction("ARRAY_POSITION(ARRAY [DATE '2000-01-01', DATE '2000-01-02', DATE '2000-01-03', DATE '2000-01-04'], DATE '2000-01-03')", BIGINT, 3L);
        assertFunction("ARRAY_POSITION(ARRAY [ARRAY [1, 11], ARRAY [2, 12], ARRAY [3, 13], ARRAY [4, 14]], ARRAY [3, 13])", BIGINT, 3L);

        assertFunction("ARRAY_POSITION(ARRAY [], NULL)", BIGINT, null);
        assertFunction("ARRAY_POSITION(ARRAY [NULL], NULL)", BIGINT, null);
        assertFunction("ARRAY_POSITION(ARRAY [1, NULL, 2], NULL)", BIGINT, null);
        assertFunction("ARRAY_POSITION(ARRAY [1, CAST(NULL AS BIGINT), 2], CAST(NULL AS BIGINT))", BIGINT, null);
        assertFunction("ARRAY_POSITION(ARRAY [1, NULL, 2], CAST(NULL AS BIGINT))", BIGINT, null);
        assertFunction("ARRAY_POSITION(ARRAY [1, CAST(NULL AS BIGINT), 2], NULL)", BIGINT, null);
        assertFunction("ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 3.0)", BIGINT, 3L);
        assertFunction("ARRAY_POSITION(ARRAY [1.0, 2.0, 000000000000000000000003.000, 4.0], 3.0)", BIGINT, 3L);
        assertFunction("ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 000000000000000000000003.000)", BIGINT, 3L);
        assertFunction("ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 3)", BIGINT, 3L);
        assertFunction("ARRAY_POSITION(ARRAY [1.0, 2.0, 3, 4.0], 4.0)", BIGINT, 4L);
    }

    @Test
    public void testSubscript()
    {
        String outOfBounds = "Array subscript out of bounds";
        String negativeIndex = "Array subscript is negative";
        String indexIsZero = "SQL array indices start at 1";
        assertInvalidFunction("ARRAY [][1]", outOfBounds);
        assertInvalidFunction("ARRAY [null][-1]", negativeIndex);
        assertInvalidFunction("ARRAY [1, 2, 3][0]", indexIsZero);
        assertInvalidFunction("ARRAY [1, 2, 3][-1]", negativeIndex);
        assertInvalidFunction("ARRAY [1, 2, 3][4]", outOfBounds);

        try {
            assertFunction("ARRAY [1, 2, 3][1.1E0]", BIGINT, null);
            fail("Access to array with double subscript should fail");
        }
        catch (SemanticException e) {
            assertTrue(e.getCode() == TYPE_MISMATCH);
        }

        assertFunction("ARRAY[NULL][1]", UNKNOWN, null);
        assertFunction("ARRAY[NULL, NULL, NULL][3]", UNKNOWN, null);
        assertFunction("1 + ARRAY [2, 1, 3][2]", INTEGER, 2);
        assertFunction("ARRAY [2, 1, 3][2]", INTEGER, 1);
        assertFunction("ARRAY [2, NULL, 3][2]", INTEGER, null);
        assertFunction("ARRAY [1.0E0, 2.5E0, 3.5E0][3]", DOUBLE, 3.5);
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]][2]", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("ARRAY [ARRAY[1, 2], NULL, ARRAY[3]][2]", new ArrayType(INTEGER), null);
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]][2][1]", INTEGER, 3);
        assertFunction("ARRAY ['puppies', 'kittens'][2]", createVarcharType(7), "kittens");
        assertFunction("ARRAY ['puppies', 'kittens', NULL][3]", createVarcharType(7), null);
        assertFunction("ARRAY [TRUE, FALSE][2]", BOOLEAN, false);
        assertFunction(
                "ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'][1]",
                TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION));
        assertFunction("ARRAY [infinity()][1]", DOUBLE, POSITIVE_INFINITY);
        assertFunction("ARRAY [-infinity()][1]", DOUBLE, NEGATIVE_INFINITY);
        assertFunction("ARRAY [sqrt(-1)][1]", DOUBLE, NaN);
        assertDecimalFunction("ARRAY [2.1, 2.2, 2.3][3]", decimal("2.3"));
        assertDecimalFunction("ARRAY [2.111111222111111114111, 2.22222222222222222, 2.222222222222223][3]", decimal("2.222222222222223000000"));
        assertDecimalFunction("ARRAY [1.9, 2, 2.3][3]", decimal("0000000002.3"));
        assertDecimalFunction("ARRAY [2.22222222222222222, 2.3][1]", decimal("2.22222222222222222"));
    }

    @Test
    public void testElementAt()
    {
        assertInvalidFunction("ELEMENT_AT(ARRAY [], 0)", "SQL array indices start at 1");
        assertInvalidFunction("ELEMENT_AT(ARRAY [1, 2, 3], 0)", "SQL array indices start at 1");

        assertFunction("ELEMENT_AT(ARRAY [], 1)", UNKNOWN, null);
        assertFunction("ELEMENT_AT(ARRAY [], -1)", UNKNOWN, null);
        assertFunction("ELEMENT_AT(ARRAY [1, 2, 3], 4)", INTEGER, null);
        assertFunction("ELEMENT_AT(ARRAY [1, 2, 3], -4)", INTEGER, null);
        assertFunction("ELEMENT_AT(ARRAY [NULL], 1)", UNKNOWN, null);
        assertFunction("ELEMENT_AT(ARRAY [NULL], -1)", UNKNOWN, null);
        assertFunction("ELEMENT_AT(ARRAY [NULL, NULL, NULL], 3)", UNKNOWN, null);
        assertFunction("ELEMENT_AT(ARRAY [NULL, NULL, NULL], -1)", UNKNOWN, null);
        assertFunction("1 + ELEMENT_AT(ARRAY [2, 1, 3], 2)", INTEGER, 2);
        assertFunction("10000000000 + ELEMENT_AT(ARRAY [2, 1, 3], -2)", BIGINT, 10000000001L);
        assertFunction("ELEMENT_AT(ARRAY [2, 1, 3], 2)", INTEGER, 1);
        assertFunction("ELEMENT_AT(ARRAY [2, 1, 3], -2)", INTEGER, 1);
        assertFunction("ELEMENT_AT(ARRAY [2, NULL, 3], 2)", INTEGER, null);
        assertFunction("ELEMENT_AT(ARRAY [2, NULL, 3], -2)", INTEGER, null);
        assertFunction("ELEMENT_AT(ARRAY [BIGINT '2', 1, 3], -2)", BIGINT, 1L);
        assertFunction("ELEMENT_AT(ARRAY [2, NULL, BIGINT '3'], -2)", BIGINT, null);
        assertFunction("ELEMENT_AT(ARRAY [1.0E0, 2.5E0, 3.5E0], 3)", DOUBLE, 3.5);
        assertFunction("ELEMENT_AT(ARRAY [1.0E0, 2.5E0, 3.5E0], -1)", DOUBLE, 3.5);
        assertFunction("ELEMENT_AT(ARRAY [ARRAY [1, 2], ARRAY [3]], 2)", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("ELEMENT_AT(ARRAY [ARRAY [1, 2], ARRAY [3]], -1)", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("ELEMENT_AT(ARRAY [ARRAY [1, 2], NULL, ARRAY [3]], 2)", new ArrayType(INTEGER), null);
        assertFunction("ELEMENT_AT(ARRAY [ARRAY [1, 2], NULL, ARRAY [3]], -2)", new ArrayType(INTEGER), null);
        assertFunction("ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], 2) , 1)", INTEGER, 3);
        assertFunction("ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], -1) , 1)", INTEGER, 3);
        assertFunction("ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], 2) , -1)", INTEGER, 3);
        assertFunction("ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], -1) , -1)", INTEGER, 3);
        assertFunction("ELEMENT_AT(ARRAY ['puppies', 'kittens'], 2)", createVarcharType(7), "kittens");
        assertFunction("ELEMENT_AT(ARRAY ['crocodiles', 'kittens'], 2)", createVarcharType(10), "kittens");
        assertFunction("ELEMENT_AT(ARRAY ['puppies', 'kittens'], -1)", createVarcharType(7), "kittens");
        assertFunction("ELEMENT_AT(ARRAY ['puppies', 'kittens', NULL], 3)", createVarcharType(7), null);
        assertFunction("ELEMENT_AT(ARRAY ['puppies', 'kittens', NULL], -1)", createVarcharType(7), null);
        assertFunction("ELEMENT_AT(ARRAY [TRUE, FALSE], 2)", BOOLEAN, false);
        assertFunction("ELEMENT_AT(ARRAY [TRUE, FALSE], -1)", BOOLEAN, false);
        assertFunction(
                "ELEMENT_AT(ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'], 1)",
                TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION));
        assertFunction(
                "ELEMENT_AT(ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'], -2)",
                TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION));
        assertFunction("ELEMENT_AT(ARRAY [infinity()], 1)", DOUBLE, POSITIVE_INFINITY);
        assertFunction("ELEMENT_AT(ARRAY [infinity()], -1)", DOUBLE, POSITIVE_INFINITY);
        assertFunction("ELEMENT_AT(ARRAY [-infinity()], 1)", DOUBLE, NEGATIVE_INFINITY);
        assertFunction("ELEMENT_AT(ARRAY [-infinity()], -1)", DOUBLE, NEGATIVE_INFINITY);
        assertFunction("ELEMENT_AT(ARRAY [sqrt(-1)], 1)", DOUBLE, NaN);
        assertFunction("ELEMENT_AT(ARRAY [sqrt(-1)], -1)", DOUBLE, NaN);
        assertDecimalFunction("ELEMENT_AT(ARRAY [2.1, 2.2, 2.3], 3)", decimal("2.3"));
        assertDecimalFunction("ELEMENT_AT(ARRAY [2.111111222111111114111, 2.22222222222222222, 2.222222222222223], 3)", decimal("2.222222222222223000000"));
        assertDecimalFunction("ELEMENT_AT(ARRAY [1.9, 2, 2.3], -1)", decimal("0000000002.3"));
        assertDecimalFunction("ELEMENT_AT(ARRAY [2.22222222222222222, 2.3], -2)", decimal("2.22222222222222222"));
    }

    @Test
    public void testShuffle()
    {
        // More tests can be found in AbstractTestQueries.testArrayShuffle

        assertCachedInstanceHasBoundedRetainedSize("SHUFFLE(ARRAY[2, 3, 4, 1])");
    }

    @Test
    public void testSort()
    {
        assertFunction("ARRAY_SORT(ARRAY[2, 3, 4, 1])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3, 4));
        assertFunction("ARRAY_SORT(ARRAY[2, BIGINT '3', 4, 1])", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 4L));
        assertFunction(
                "ARRAY_SORT(ARRAY [2.3, 2.1, 2.2])",
                new ArrayType(createDecimalType(2, 1)),
                ImmutableList.of(decimal("2.1"), decimal("2.2"), decimal("2.3")));
        assertFunction(
                "ARRAY_SORT(ARRAY [2, 1.900, 2.330])",
                new ArrayType(createDecimalType(13, 3)),
                ImmutableList.of(decimal("0000000001.900"), decimal("0000000002.000"), decimal("0000000002.330")));
        assertFunction("ARRAY_SORT(ARRAY['z', 'f', 's', 'd', 'g'])", new ArrayType(createVarcharType(1)), ImmutableList.of("d", "f", "g", "s", "z"));
        assertFunction("ARRAY_SORT(ARRAY[TRUE, FALSE])", new ArrayType(BOOLEAN), ImmutableList.of(false, true));
        assertFunction("ARRAY_SORT(ARRAY[22.1E0, 11.1E0, 1.1E0, 44.1E0])", new ArrayType(DOUBLE), ImmutableList.of(1.1, 11.1, 22.1, 44.1));
        assertFunction(
                "ARRAY_SORT(ARRAY [TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1989-02-06 12:00:00'])",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(1989, 2, 6, 12, 0, 0, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction("ARRAY_SORT(ARRAY [ARRAY [1], ARRAY [2]])",
                new ArrayType(new ArrayType(INTEGER)),
                ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2)));

        // with lambda function
        assertFunction(
                "ARRAY_SORT(ARRAY[2, 3, 2, null, null, 4, 1], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(INTEGER),
                asList(null, null, 4, 3, 2, 2, 1));
        assertFunction(
                "ARRAY_SORT(ARRAY[2, 3, 2, null, null, 4, 1], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN 1 " +
                        "WHEN y IS NULL THEN -1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(INTEGER),
                asList(4, 3, 2, 2, 1, null, null));
        assertFunction(
                "ARRAY_SORT(ARRAY[2, null, BIGINT '3', 4, null, 1], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(BIGINT),
                asList(null, null, 4L, 3L, 2L, 1L));
        assertFunction(
                "ARRAY_SORT(ARRAY['bc', null, 'ab', 'dc', null], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(createVarcharType(2)),
                asList(null, null, "dc", "bc", "ab"));
        assertFunction(
                "ARRAY_SORT(ARRAY['a', null, 'abcd', null, 'abc', 'zx'], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN length(x) < length(y) THEN 1 " +
                        "WHEN length(x) = length(y) THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(createVarcharType(4)),
                asList(null, null, "abcd", "abc", "zx", "a"));
        assertFunction(
                "ARRAY_SORT(ARRAY[TRUE, null, FALSE, TRUE, null, FALSE, TRUE], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "WHEN x THEN -1 " +
                        "ELSE 1 END)",
                new ArrayType(BOOLEAN),
                asList(null, null, true, true, true, false, false));
        assertFunction(
                "ARRAY_SORT(ARRAY[22.1E0, null, null, 11.1E0, 1.1E0, 44.1E0], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(DOUBLE),
                asList(null, null, 44.1, 22.1, 11.1, 1.1));
        assertFunction(

                "ARRAY_SORT(ARRAY[TIMESTAMP '1973-07-08 22:00:01', NULL, TIMESTAMP '1970-01-01 00:00:01', NULL, TIMESTAMP '1989-02-06 12:00:00'], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN date_diff('millisecond', y, x) < 0 THEN 1 " +
                        "WHEN date_diff('millisecond', y, x) = 0 THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(TIMESTAMP),
                asList(
                        null,
                        null,
                        sqlTimestampOf(1989, 2, 6, 12, 0, 0, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction(
                "ARRAY_SORT(ARRAY[ARRAY[2, 3, 1], null, ARRAY[4, null, 2, 1, 4], ARRAY[1, 2], null], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN cardinality(x) < cardinality(y) THEN 1 " +
                        "WHEN cardinality(x) = cardinality(y) THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(new ArrayType(INTEGER)),
                asList(null, null, asList(4, null, 2, 1, 4), asList(2, 3, 1), asList(1, 2)));
        assertFunction(
                "ARRAY_SORT(ARRAY[2.3, null, 2.1, null, 2.2], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                new ArrayType(createDecimalType(2, 1)),
                asList(null, null, decimal("2.3"), decimal("2.2"), decimal("2.1")));

        // with null in the array, should be in nulls-last order
        List<Integer> expected = asList(-1, 0, 1, null, null);
        assertFunction("ARRAY_SORT(ARRAY[1, null, 0, null, -1])", new ArrayType(INTEGER), expected);
        assertFunction("ARRAY_SORT(ARRAY[1, null, null, -1, 0])", new ArrayType(INTEGER), expected);

        // invalid functions
        assertInvalidFunction("ARRAY_SORT(ARRAY[color('red'), color('blue')])", FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "ARRAY_SORT(ARRAY[2, 1, 2, 4], (x, y) -> y - x)",
                INVALID_FUNCTION_ARGUMENT,
                "Lambda comparator must return either -1, 0, or 1");
        assertInvalidFunction(
                "ARRAY_SORT(ARRAY[1, 2], (x, y) -> x / COALESCE(y, 0))",
                INVALID_FUNCTION_ARGUMENT,
                "Lambda comparator must return either -1, 0, or 1");
        assertInvalidFunction(
                "ARRAY_SORT(ARRAY[2, 3, 2, 4, 1], (x, y) -> IF(x > y, NULL, IF(x = y, 0, -1)))",
                INVALID_FUNCTION_ARGUMENT,
                "Lambda comparator must return either -1, 0, or 1");
        assertInvalidFunction(
                "ARRAY_SORT(ARRAY[1, null], (x, y) -> x / COALESCE(y, 0))",
                INVALID_FUNCTION_ARGUMENT,
                "Lambda comparator must return either -1, 0, or 1");

        assertCachedInstanceHasBoundedRetainedSize("ARRAY_SORT(ARRAY[2, 3, 4, 1])");
    }

    @Test
    public void testReverse()
    {
        assertFunction("REVERSE(ARRAY[1])", new ArrayType(INTEGER), ImmutableList.of(1));
        assertFunction("REVERSE(ARRAY[1, 2, 3, 4])", new ArrayType(INTEGER), ImmutableList.of(4, 3, 2, 1));
        assertFunction("REVERSE(ARRAY_SORT(ARRAY[2, 3, 4, 1]))", new ArrayType(INTEGER), ImmutableList.of(4, 3, 2, 1));
        assertFunction("REVERSE(ARRAY[2, BIGINT '3', 4, 1])", new ArrayType(BIGINT), ImmutableList.of(1L, 4L, 3L, 2L));
        assertFunction("REVERSE(ARRAY['a', 'b', 'c', 'd'])", new ArrayType(createVarcharType(1)), ImmutableList.of("d", "c", "b", "a"));
        assertFunction("REVERSE(ARRAY[TRUE, FALSE])", new ArrayType(BOOLEAN), ImmutableList.of(false, true));
        assertFunction("REVERSE(ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0])", new ArrayType(DOUBLE), ImmutableList.of(4.4, 3.3, 2.2, 1.1));
        assertCachedInstanceHasBoundedRetainedSize("REVERSE(ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0])");
    }

    @Test
    public void testDistinct()
    {
        assertFunction("ARRAY_DISTINCT(ARRAY [])", new ArrayType(UNKNOWN), ImmutableList.of());

        // Order matters here. Result should be stable.
        assertFunction("ARRAY_DISTINCT(ARRAY [2, 3, 4, 3, 1, 2, 3])", new ArrayType(INTEGER), ImmutableList.of(2, 3, 4, 1));
        assertFunction("ARRAY_DISTINCT(ARRAY [2.2E0, 3.3E0, 4.4E0, 3.3E0, 1, 2.2E0, 3.3E0])", new ArrayType(DOUBLE), ImmutableList.of(2.2, 3.3, 4.4, 1.0));
        assertFunction("ARRAY_DISTINCT(ARRAY [TRUE, TRUE, TRUE])", new ArrayType(BOOLEAN), ImmutableList.of(true));
        assertFunction("ARRAY_DISTINCT(ARRAY [TRUE, FALSE, FALSE, TRUE])", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction(
                "ARRAY_DISTINCT(ARRAY [TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'])",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(1973, 7, 8, 22, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction("ARRAY_DISTINCT(ARRAY ['2', '3', '2'])", new ArrayType(createVarcharType(1)), ImmutableList.of("2", "3"));
        assertFunction("ARRAY_DISTINCT(ARRAY ['BB', 'CCC', 'BB'])", new ArrayType(createVarcharType(3)), ImmutableList.of("BB", "CCC"));
        assertFunction(
                "ARRAY_DISTINCT(ARRAY [ARRAY [1], ARRAY [1, 2], ARRAY [1, 2, 3], ARRAY [1, 2]])",
                new ArrayType(new ArrayType(INTEGER)),
                ImmutableList.of(ImmutableList.of(1), ImmutableList.of(1, 2), ImmutableList.of(1, 2, 3)));
        assertFunction("ARRAY_DISTINCT(ARRAY [NULL, 2.2E0, 3.3E0, 4.4E0, 3.3E0, 1, 2.2E0, 3.3E0])", new ArrayType(DOUBLE), asList(null, 2.2, 3.3, 4.4, 1.0));
        assertFunction("ARRAY_DISTINCT(ARRAY [2, 3, NULL, 4, 3, 1, 2, 3])", new ArrayType(INTEGER), asList(2, 3, null, 4, 1));
        assertFunction("ARRAY_DISTINCT(ARRAY ['BB', 'CCC', 'BB', NULL])", new ArrayType(createVarcharType(3)), asList("BB", "CCC", null));
        assertFunction("ARRAY_DISTINCT(ARRAY [NULL])", new ArrayType(UNKNOWN), asList((Object) null));
        assertFunction("ARRAY_DISTINCT(ARRAY [NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));
        assertFunction("ARRAY_DISTINCT(ARRAY [NULL, NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));

        // Test for BIGINT-optimized implementation
        assertFunction("ARRAY_DISTINCT(ARRAY [CAST(5 AS BIGINT), NULL, CAST(12 AS BIGINT), NULL])", new ArrayType(BIGINT), asList(5L, null, 12L));
        assertFunction("ARRAY_DISTINCT(ARRAY [CAST(100 AS BIGINT), NULL, CAST(100 AS BIGINT), NULL, 0, -2, 0])", new ArrayType(BIGINT), asList(100L, null, 0L, -2L));

        assertFunction(
                "ARRAY_DISTINCT(ARRAY [2.3, 2.3, 2.2])",
                new ArrayType(createDecimalType(2, 1)),
                ImmutableList.of(decimal("2.3"), decimal("2.2")));
        assertFunction(
                "ARRAY_DISTINCT(ARRAY [2.330, 1.900, 2.330])",
                new ArrayType(createDecimalType(4, 3)),
                ImmutableList.of(decimal("2.330"), decimal("1.900")));

        assertCachedInstanceHasBoundedRetainedSize("ARRAY_DISTINCT(ARRAY[2, 3, 4, 1, 2])");
        assertCachedInstanceHasBoundedRetainedSize("ARRAY_DISTINCT(ARRAY[CAST(5 AS BIGINT), NULL, CAST(12 AS BIGINT), NULL])");
        assertCachedInstanceHasBoundedRetainedSize("ARRAY_DISTINCT(ARRAY[true, true, false, true, false])");
        assertCachedInstanceHasBoundedRetainedSize("ARRAY_DISTINCT(ARRAY['cat', 'dog', 'dog', 'coffee', 'apple'])");
    }

    @Test
    public void testSlice()
    {
        assertFunction("SLICE(ARRAY [1, 2, 3, 4, 5], 1, 4)", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3, 4));
        assertFunction("SLICE(ARRAY [1, 2], 1, 4)", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4, 5], 3, 2)", new ArrayType(INTEGER), ImmutableList.of(3, 4));
        assertFunction("SLICE(ARRAY ['1', '2', '3', '4'], 2, 1)", new ArrayType(createVarcharType(1)), ImmutableList.of("2"));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], 3, 3)", new ArrayType(INTEGER), ImmutableList.of(3, 4));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], -3, 3)", new ArrayType(INTEGER), ImmutableList.of(2, 3, 4));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], -3, 5)", new ArrayType(INTEGER), ImmutableList.of(2, 3, 4));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], 1, 0)", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], -2, 0)", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], -5, 5)", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], -6, 5)", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("SLICE(ARRAY [ARRAY [1], ARRAY [2, 3], ARRAY [4, 5, 6]], 1, 2)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 3)));
        assertFunction("SLICE(ARRAY [2.3, 2.3, 2.2], 2, 3)", new ArrayType(createDecimalType(2, 1)), ImmutableList.of(decimal("2.3"), decimal("2.2")));
        assertFunction("SLICE(ARRAY [2.330, 1.900, 2.330], 1, 2)", new ArrayType(createDecimalType(4, 3)), ImmutableList.of(decimal("2.330"), decimal("1.900")));

        assertInvalidFunction("SLICE(ARRAY [1, 2, 3, 4], 1, -1)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("SLICE(ARRAY [1, 2, 3, 4], 0, 1)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testArraysOverlap()
    {
        assertFunction("ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [2, 3])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [2, 1], ARRAY [2, 3])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [2, 1], ARRAY [3, 2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [3, 2])", BooleanType.BOOLEAN, true);

        assertFunction("ARRAYS_OVERLAP(ARRAY [1, 3], ARRAY [2, 4])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [3, 1], ARRAY [2, 4])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [3, 1], ARRAY [4, 2])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [1, 3], ARRAY [4, 2])", BooleanType.BOOLEAN, false);

        assertFunction("ARRAYS_OVERLAP(ARRAY [1, 3], ARRAY [2, 3, 4])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [3, 1], ARRAY [5, 4, 1])", BooleanType.BOOLEAN, true);

        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 2], ARRAY [CAST(2 AS BIGINT), 3])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), 1], ARRAY [CAST(2 AS BIGINT), 3])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), 1], ARRAY [CAST(3 AS BIGINT), 2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 2], ARRAY [CAST(3 AS BIGINT), 2])", BooleanType.BOOLEAN, true);

        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 3], ARRAY [CAST(2 AS BIGINT), 4])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(3 AS BIGINT), 1], ARRAY [CAST(2 AS BIGINT), 4])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(3 AS BIGINT), 1], ARRAY [CAST(4 AS BIGINT), 2])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 3], ARRAY [CAST(4 AS BIGINT), 2])", BooleanType.BOOLEAN, false);

        assertFunction("ARRAYS_OVERLAP(ARRAY ['dog', 'cat'], ARRAY ['monkey', 'dog'])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY ['dog', 'cat'], ARRAY ['monkey', 'fox'])", BooleanType.BOOLEAN, false);

        // Test arrays with NULLs
        assertFunction("ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [NULL, 2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [2, NULL])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [2, 1], ARRAY [NULL, 3])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [2, 1], ARRAY [3, NULL])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [NULL, 2], ARRAY [1, 2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [2, NULL], ARRAY [1, 2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [NULL, 3], ARRAY [2, 1])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [3, NULL], ARRAY [2, 1])", BooleanType.BOOLEAN, null);

        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 2], ARRAY [NULL, CAST(2 AS BIGINT)])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 2], ARRAY [CAST(2 AS BIGINT), NULL])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), 1], ARRAY [CAST(3 AS BIGINT), NULL])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), 1], ARRAY [NULL, CAST(3 AS BIGINT)])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [NULL, CAST(2 AS BIGINT)], ARRAY [CAST(1 AS BIGINT), 2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), NULL], ARRAY [CAST(1 AS BIGINT), 2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [CAST(3 AS BIGINT), NULL], ARRAY [CAST(2 AS BIGINT), 1])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [NULL, CAST(3 AS BIGINT)], ARRAY [CAST(2 AS BIGINT), 1])", BooleanType.BOOLEAN, null);

        assertFunction("ARRAYS_OVERLAP(ARRAY ['dog', 'cat'], ARRAY [NULL, 'dog'])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY ['dog', 'cat'], ARRAY ['monkey', NULL])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [NULL, 'dog'], ARRAY ['dog', 'cat'])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY ['monkey', NULL], ARRAY ['dog', 'cat'])", BooleanType.BOOLEAN, null);

        assertFunction("ARRAYS_OVERLAP(ARRAY [ARRAY [1, 2], ARRAY[3]], ARRAY [ARRAY[4], ARRAY [1, 2]])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [ARRAY [1, 2], ARRAY[3]], ARRAY [ARRAY[4], NULL])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [ARRAY [2], ARRAY[3]], ARRAY [ARRAY[4], ARRAY[1, 2]])", BooleanType.BOOLEAN, false);

        assertFunction("ARRAYS_OVERLAP(ARRAY [], ARRAY [])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [], ARRAY [1, 2])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [], ARRAY [NULL])", BooleanType.BOOLEAN, false);

        assertFunction("ARRAYS_OVERLAP(ARRAY [true], ARRAY [true, false])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAYS_OVERLAP(ARRAY [false], ARRAY [true, true])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAYS_OVERLAP(ARRAY [true, false], ARRAY [NULL])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAYS_OVERLAP(ARRAY [false], ARRAY [true, NULL])", BooleanType.BOOLEAN, null);
    }

    @Test
    public void testArrayIntersect()
    {
        assertFunction("ARRAY_INTERSECT(ARRAY [12], ARRAY [10])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])", new ArrayType(createVarcharType(4)), ImmutableList.of("bar", "foo"));
        assertFunction("ARRAY_INTERSECT(ARRAY [NULL], ARRAY [NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));
        assertFunction("ARRAY_INTERSECT(ARRAY ['abc', NULL, 'xyz', NULL], ARRAY [NULL, 'abc', NULL, NULL])", new ArrayType(createVarcharType(3)), asList(null, "abc"));
        assertFunction("ARRAY_INTERSECT(ARRAY [1, 5], ARRAY [1])", new ArrayType(INTEGER), ImmutableList.of(1));
        assertFunction("ARRAY_INTERSECT(ARRAY [1, 1, 2, 4], ARRAY [1, 1, 4, 4])", new ArrayType(INTEGER), ImmutableList.of(1, 4));
        assertFunction("ARRAY_INTERSECT(ARRAY [2, 8], ARRAY [8, 3])", new ArrayType(INTEGER), ImmutableList.of(8));
        assertFunction("ARRAY_INTERSECT(ARRAY [IF (RAND() < 1.0E0, 7, 1) , 2], ARRAY [7])", new ArrayType(INTEGER), ImmutableList.of(7));
        assertFunction("ARRAY_INTERSECT(ARRAY [1, 5], ARRAY [1.0E0])", new ArrayType(DOUBLE), ImmutableList.of(1.0));
        assertFunction("ARRAY_INTERSECT(ARRAY [8.3E0, 1.6E0, 4.1E0, 5.2E0], ARRAY [4.0E0, 5.2E0, 8.3E0, 9.7E0, 3.5E0])", new ArrayType(DOUBLE), ImmutableList.of(5.2, 8.3));
        assertFunction("ARRAY_INTERSECT(ARRAY [5.1E0, 7, 3.0E0, 4.8E0, 10], ARRAY [6.5E0, 10.0E0, 1.9E0, 5.1E0, 3.9E0, 4.8E0])", new ArrayType(DOUBLE), ImmutableList.of(4.8, 5.1, 10.0));
        assertFunction("ARRAY_INTERSECT(ARRAY [ARRAY [4, 5], ARRAY [6, 7]], ARRAY [ARRAY [4, 5], ARRAY [6, 8]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(4, 5)));

        assertCachedInstanceHasBoundedRetainedSize("ARRAY_INTERSECT(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])");

        assertFunction(
                "ARRAY_INTERSECT(ARRAY [2.3, 2.3, 2.2], ARRAY[2.2, 2.3])",
                new ArrayType(createDecimalType(2, 1)),
                ImmutableList.of(decimal("2.2"), decimal("2.3")));
        assertFunction("ARRAY_INTERSECT(ARRAY [2.330, 1.900, 2.330], ARRAY [2.3300, 1.9000])", new ArrayType(createDecimalType(5, 4)),
                ImmutableList.of(decimal("1.9000"), decimal("2.3300")));
        assertFunction("ARRAY_INTERSECT(ARRAY [2, 3], ARRAY[2.0, 3.0])", new ArrayType(createDecimalType(11, 1)),
                ImmutableList.of(decimal("00000000002.0"), decimal("00000000003.0")));
    }

    @Test
    public void testArrayUnion()
    {
        assertFunction("ARRAY_UNION(ARRAY [cast(10 as bigint), NULL, cast(12 as bigint), NULL], ARRAY [NULL, cast(10 as bigint), NULL, NULL])", new ArrayType(BIGINT), asList(10L, null, 12L));
        assertFunction("ARRAY_UNION(ARRAY [12], ARRAY [10])", new ArrayType(INTEGER), ImmutableList.of(12, 10));
        assertFunction("ARRAY_UNION(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])", new ArrayType(createVarcharType(4)), ImmutableList.of("foo", "bar", "baz", "test"));
        assertFunction("ARRAY_UNION(ARRAY [NULL], ARRAY [NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));
        assertFunction("ARRAY_UNION(ARRAY ['abc', NULL, 'xyz', NULL], ARRAY [NULL, 'abc', NULL, NULL])", new ArrayType(createVarcharType(3)), asList("abc", null, "xyz"));
        assertFunction("ARRAY_UNION(ARRAY [1, 5], ARRAY [1])", new ArrayType(INTEGER), ImmutableList.of(1, 5));
        assertFunction("ARRAY_UNION(ARRAY [1, 1, 2, 4], ARRAY [1, 1, 4, 4])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 4));
        assertFunction("ARRAY_UNION(ARRAY [2, 8], ARRAY [8, 3])", new ArrayType(INTEGER), ImmutableList.of(2, 8, 3));
        assertFunction("ARRAY_UNION(ARRAY [IF (RAND() < 1.0E0, 7, 1) , 2], ARRAY [7])", new ArrayType(INTEGER), ImmutableList.of(7, 2));
        assertFunction("ARRAY_UNION(ARRAY [1, 5], ARRAY [1.0E0])", new ArrayType(DOUBLE), ImmutableList.of(1.0, 5.0));
        assertFunction("ARRAY_UNION(ARRAY [8.3E0, 1.6E0, 4.1E0, 5.2E0], ARRAY [4.0E0, 5.2E0, 8.3E0, 9.7E0, 3.5E0])", new ArrayType(DOUBLE), ImmutableList.of(8.3, 1.6, 4.1, 5.2, 4.0, 9.7, 3.5));
        assertFunction("ARRAY_UNION(ARRAY [5.1E0, 7, 3.0E0, 4.8E0, 10], ARRAY [6.5E0, 10.0E0, 1.9E0, 5.1E0, 3.9E0, 4.8E0])", new ArrayType(DOUBLE), ImmutableList.of(5.1, 7.0, 3.0, 4.8, 10.0, 6.5, 1.9, 3.9));
        assertFunction("ARRAY_UNION(ARRAY [ARRAY [4, 5], ARRAY [6, 7]], ARRAY [ARRAY [4, 5], ARRAY [6, 8]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(4, 5), ImmutableList.of(6, 7), ImmutableList.of(6, 8)));
    }

    @Test
    public void testComparison()
    {
        assertFunction("ARRAY [1, 2, 3] = ARRAY [1, 2, 3]", BOOLEAN, true);
        assertFunction("ARRAY [1, 2, 3] != ARRAY [1, 2, 3]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] = ARRAY [TRUE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] != ARRAY [TRUE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] = ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] != ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] = ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] != ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [timestamp '2012-10-31 08:00 UTC'] = ARRAY [timestamp '2012-10-31 01:00 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [timestamp '2012-10-31 08:00 UTC'] != ARRAY [timestamp '2012-10-31 01:00 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, false);
        assertFunction("ARRAY [1.0, 2.0, 3.0] = ARRAY [1.0, 2.0, 3.0]", BOOLEAN, true);
        assertFunction("ARRAY [1.0, 2.0, 3.0] = ARRAY [1.0, 2.0, 3.1]", BOOLEAN, false);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "= ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]", BOOLEAN, true);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "= ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]", BOOLEAN, false);
        assertFunction("ARRAY [1.0, 2.0, 3.0] != ARRAY [1.0, 2.0, 3.0]", BOOLEAN, false);
        assertFunction("ARRAY [1.0, 2.0, 3.0] != ARRAY [1.0, 2.0, 3.1]", BOOLEAN, true);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "!= ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]", BOOLEAN, false);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "!= ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]", BOOLEAN, true);

        assertFunction("ARRAY [10, 20, 30] != ARRAY [5]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] = ARRAY [5]", BOOLEAN, false);
        assertFunction("ARRAY [1, 2, 3] != ARRAY [3, 2, 1]", BOOLEAN, true);
        assertFunction("ARRAY [1, 2, 3] = ARRAY [3, 2, 1]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] != ARRAY [TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] = ARRAY [TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] != ARRAY [FALSE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] = ARRAY [FALSE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] != ARRAY [1.1E0, 2.2E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] = ARRAY [1.1E0, 2.2E0]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0] != ARRAY [11.1E0, 22.1E0, 1.1E0, 44.1E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0] = ARRAY [11.1E0, 22.1E0, 1.1E0, 44.1E0]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] != ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] = ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] != ARRAY ['z', 'f', 's', 'd', 'g']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] = ARRAY ['z', 'f', 's', 'd', 'g']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2, 3], ARRAY [4, 5]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2, 3], ARRAY [4, 5]]", BOOLEAN, false);
        assertFunction("ARRAY [1.0, 2.0, 3.0] = ARRAY [1.0, 2.0]", BOOLEAN, false);
        assertFunction("ARRAY [1.0, 2.0, 3.0] != ARRAY [1.0, 2.0]", BOOLEAN, true);

        assertFunction("ARRAY [10, 20, 30] < ARRAY [10, 20, 40, 50]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] >= ARRAY [10, 20, 40, 50]", BOOLEAN, false);
        assertFunction("ARRAY [10, 20, 30] < ARRAY [10, 40]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] >= ARRAY [10, 40]", BOOLEAN, false);
        assertFunction("ARRAY [10, 20] < ARRAY [10, 20, 30]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20] >= ARRAY [10, 20, 30]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] < ARRAY [TRUE, TRUE, TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] >= ARRAY [TRUE, TRUE, TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE, FALSE] < ARRAY [TRUE, TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE, FALSE] >= ARRAY [TRUE, TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] < ARRAY [TRUE, FALSE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] >= ARRAY [TRUE, FALSE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] < ARRAY [1.1E0, 2.2E0, 4.4E0, 4.4E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] >= ARRAY [1.1E0, 2.2E0, 4.4E0, 4.4E0]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] < ARRAY [1.1E0, 2.2E0, 5.5E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] >= ARRAY [1.1E0, 2.2E0, 5.5E0]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0] < ARRAY [1.1E0, 2.2E0, 5.5E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0] >= ARRAY [1.1E0, 2.2E0, 5.5E0]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'lizards', 'lizards']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'lizards', 'lizards']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'lizards']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'lizards']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] < ARRAY ['puppies', 'kittens', 'lizards']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] >= ARRAY ['puppies', 'kittens', 'lizards']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]", BOOLEAN, false);
        assertFunction("ARRAY [1.0, 2.0, 3.0] > ARRAY [1.0, 2.0, 3.0]", BOOLEAN, false);
        assertFunction("ARRAY [1.0, 2.0, 3.0] >= ARRAY [1.0, 2.0, 3.0]", BOOLEAN, true);
        assertFunction("ARRAY [1.0, 2.0, 3.0] < ARRAY [1.0, 2.0, 3.1]", BOOLEAN, true);
        assertFunction("ARRAY [1.0, 2.0, 3.0] <= ARRAY [1.0, 2.0, 3.1]", BOOLEAN, true);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "> ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]", BOOLEAN, false);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                ">= ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]", BOOLEAN, true);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "< ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]", BOOLEAN, false);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210] " +
                "< ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]", BOOLEAN, true);
        assertFunction("ARRAY [1234567890.1234567890, 0] " +
                "< ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]", BOOLEAN, true);
        assertFunction("ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "<= ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]", BOOLEAN, true);

        assertFunction("ARRAY [10, 20, 30] > ARRAY [10, 20, 20]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] <= ARRAY [10, 20, 20]", BOOLEAN, false);
        assertFunction("ARRAY [10, 20, 30] > ARRAY [10, 20]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] <= ARRAY [10, 20]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, TRUE, TRUE] > ARRAY [TRUE, TRUE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, TRUE, TRUE] <= ARRAY [TRUE, TRUE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, TRUE, FALSE] > ARRAY [TRUE, TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, TRUE, FALSE] <= ARRAY [TRUE, TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] > ARRAY [1.1E0, 2.2E0, 2.2E0, 4.4E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] <= ARRAY [1.1E0, 2.2E0, 2.2E0, 4.4E0]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] > ARRAY [1.1E0, 2.2E0, 3.3E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] <= ARRAY [1.1E0, 2.2E0, 3.3E0]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'kittens', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'kittens', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:20.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:20.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 4]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 4]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 3, 4]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 3, 4]]", BOOLEAN, false);

        assertFunction("ARRAY [10, 20, 30] <= ARRAY [50]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] > ARRAY [50]", BOOLEAN, false);
        assertFunction("ARRAY [10, 20, 30] <= ARRAY [10, 20, 30]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] > ARRAY [10, 20, 30]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] <= ARRAY [TRUE, FALSE, true]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] > ARRAY [TRUE, FALSE, true]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] <= ARRAY [TRUE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] > ARRAY [TRUE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] <= ARRAY [2.2E0, 5.5E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] > ARRAY [2.2E0, 5.5E0]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] <= ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] > ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'lizards']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'lizards']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] <= ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] > ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]", BOOLEAN, false);

        assertFunction("ARRAY [10, 20, 30] >= ARRAY [10, 20, 30]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] < ARRAY [10, 20, 30]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] >= ARRAY [TRUE, FALSE, TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] < ARRAY [TRUE, FALSE, TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] >= ARRAY [TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] < ARRAY [TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] >= ARRAY [1.1E0, 2.2E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] < ARRAY [1.1E0, 2.2E0]", BOOLEAN, false);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] >= ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]", BOOLEAN, true);
        assertFunction("ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] < ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'kittens', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'kittens', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] >= ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] < ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, false);

        assertInvalidFunction("ARRAY [1, NULL] = ARRAY [1, 2]", NOT_SUPPORTED.toErrorCode());
    }

    @Test
    public void testDistinctFrom()
    {
        assertFunction("CAST(NULL AS ARRAY(UNKNOWN)) IS DISTINCT FROM CAST(NULL AS ARRAY(UNKNOWN))", BOOLEAN, false);
        assertFunction("ARRAY [NULL] IS DISTINCT FROM ARRAY [NULL]", BOOLEAN, false);
        assertFunction("NULL IS DISTINCT FROM ARRAY [1, 2]", BOOLEAN, true);
        assertFunction("ARRAY [1, 2] IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("ARRAY [1, 2] IS DISTINCT FROM ARRAY [1, 2]", BOOLEAN, false);
        assertFunction("ARRAY [1, 2, 3] IS DISTINCT FROM ARRAY [1, 2]", BOOLEAN, true);
        assertFunction("ARRAY [1, 2] IS DISTINCT FROM ARRAY [1, NULL]", BOOLEAN, true);
        assertFunction("ARRAY [1, 2] IS DISTINCT FROM ARRAY [1, 3]", BOOLEAN, true);
        assertFunction("ARRAY [1, NULL] IS DISTINCT FROM ARRAY [1, NULL]", BOOLEAN, false);
        assertFunction("ARRAY [1, NULL] IS DISTINCT FROM ARRAY [1, NULL]", BOOLEAN, false);
        assertFunction("ARRAY [1, 2, NULL] IS DISTINCT FROM ARRAY [1, 2]", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] IS DISTINCT FROM ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', NULL] IS DISTINCT FROM ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', NULL] IS DISTINCT FROM ARRAY [NULL, 'kittens']", BOOLEAN, true);
    }

    @Test
    public void testArrayRemove()
    {
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'foo')", new ArrayType(createVarcharType(3)), ImmutableList.of("bar", "baz"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'bar')", new ArrayType(createVarcharType(3)), ImmutableList.of("foo", "baz"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'baz')", new ArrayType(createVarcharType(3)), ImmutableList.of("foo", "bar"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'zzz')", new ArrayType(createVarcharType(3)), ImmutableList.of("foo", "bar", "baz"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'foo', 'foo'], 'foo')", new ArrayType(createVarcharType(3)), ImmutableList.of());
        assertFunction("ARRAY_REMOVE(ARRAY [NULL, 'bar', 'baz'], 'foo')", new ArrayType(createVarcharType(3)), asList(null, "bar", "baz"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', NULL], 'foo')", new ArrayType(createVarcharType(3)), asList("bar", null));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 2, 3], 1)", new ArrayType(INTEGER), ImmutableList.of(2, 3));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 2, 3], 2)", new ArrayType(INTEGER), ImmutableList.of(1, 3));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 2, 3], 3)", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 2, 3], 4)", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 1, 1], 1)", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("ARRAY_REMOVE(ARRAY [NULL, 2, 3], 1)", new ArrayType(INTEGER), asList(null, 2, 3));
        assertFunction("ARRAY_REMOVE(ARRAY [1, NULL, 3], 1)", new ArrayType(INTEGER), asList(null, 3));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23E0, 3.14E0], 3.14E0)", new ArrayType(DOUBLE), ImmutableList.of(-1.23));
        assertFunction("ARRAY_REMOVE(ARRAY [3.14E0], 0.0E0)", new ArrayType(DOUBLE), ImmutableList.of(3.14));
        assertFunction("ARRAY_REMOVE(ARRAY [sqrt(-1), 3.14E0], 3.14E0)", new ArrayType(DOUBLE), ImmutableList.of(NaN));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23E0, sqrt(-1)], nan())", new ArrayType(DOUBLE), ImmutableList.of(-1.23, NaN));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23E0, nan()], nan())", new ArrayType(DOUBLE), ImmutableList.of(-1.23, NaN));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23E0, infinity()], -1.23E0)", new ArrayType(DOUBLE), ImmutableList.of(POSITIVE_INFINITY));
        assertFunction("ARRAY_REMOVE(ARRAY [infinity(), 3.14E0], infinity())", new ArrayType(DOUBLE), ImmutableList.of(3.14));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23E0, NULL, 3.14E0], 3.14E0)", new ArrayType(DOUBLE), asList(-1.23, null));
        assertFunction("ARRAY_REMOVE(ARRAY [TRUE, FALSE, TRUE], TRUE)", new ArrayType(BOOLEAN), ImmutableList.of(false));
        assertFunction("ARRAY_REMOVE(ARRAY [TRUE, FALSE, TRUE], FALSE)", new ArrayType(BOOLEAN), ImmutableList.of(true, true));
        assertFunction("ARRAY_REMOVE(ARRAY [NULL, FALSE, TRUE], TRUE)", new ArrayType(BOOLEAN), asList(null, false));
        assertFunction("ARRAY_REMOVE(ARRAY [ARRAY ['foo'], ARRAY ['bar'], ARRAY ['baz']], ARRAY ['bar'])", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(ImmutableList.of("foo"), ImmutableList.of("baz")));
        assertFunction(
                "ARRAY_REMOVE(ARRAY [1.0, 2.0, 3.0], 2.0)",
                new ArrayType(createDecimalType(2, 1)),
                ImmutableList.of(decimal("1.0"), decimal("3.0")));
        assertFunction(
                "ARRAY_REMOVE(ARRAY [1.0, 2.0, 3.0], 4.0)",
                new ArrayType(createDecimalType(2, 1)),
                ImmutableList.of(decimal("1.0"), decimal("2.0"), decimal("3.0")));
        assertFunction(
                "ARRAY_REMOVE(ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543], 1234567890.1234567890)",
                new ArrayType(createDecimalType(22, 10)),
                ImmutableList.of(decimal("9876543210.9876543210"), decimal("123123123456.6549876543")));
        assertFunction(
                "ARRAY_REMOVE(ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543], 4.0)",
                new ArrayType(createDecimalType(22, 10)),
                ImmutableList.of(decimal("1234567890.1234567890"), decimal("9876543210.9876543210"), decimal("123123123456.6549876543")));
        assertCachedInstanceHasBoundedRetainedSize("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'foo')");
    }

    @Test
    public void testRepeat()
    {
        // concrete values
        assertFunction("REPEAT(1, 5)", new ArrayType(INTEGER), ImmutableList.of(1, 1, 1, 1, 1));
        assertFunction("REPEAT('varchar', 3)", new ArrayType(createVarcharType(7)), ImmutableList.of("varchar", "varchar", "varchar"));
        assertFunction("REPEAT(true, 1)", new ArrayType(BOOLEAN), ImmutableList.of(true));
        assertFunction("REPEAT(0.5E0, 4)", new ArrayType(DOUBLE), ImmutableList.of(0.5, 0.5, 0.5, 0.5));
        assertFunction("REPEAT(array[1], 4)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1), ImmutableList.of(1), ImmutableList.of(1), ImmutableList.of(1)));

        // null values
        assertFunction("REPEAT(null, 4)", new ArrayType(UNKNOWN), asList(null, null, null, null));
        assertFunction("REPEAT(cast(null as bigint), 4)", new ArrayType(BIGINT), asList(null, null, null, null));
        assertFunction("REPEAT(cast(null as double), 4)", new ArrayType(DOUBLE), asList(null, null, null, null));
        assertFunction("REPEAT(cast(null as varchar), 4)", new ArrayType(VARCHAR), asList(null, null, null, null));
        assertFunction("REPEAT(cast(null as boolean), 4)", new ArrayType(BOOLEAN), asList(null, null, null, null));
        assertFunction("REPEAT(cast(null as array(boolean)), 4)", new ArrayType(new ArrayType(BOOLEAN)), asList(null, null, null, null));

        // 0 counts
        assertFunction("REPEAT(cast(null as bigint), 0)", new ArrayType(BIGINT), ImmutableList.of());
        assertFunction("REPEAT(1, 0)", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("REPEAT('varchar', 0)", new ArrayType(createVarcharType(7)), ImmutableList.of());
        assertFunction("REPEAT(true, 0)", new ArrayType(BOOLEAN), ImmutableList.of());
        assertFunction("REPEAT(0.5E0, 0)", new ArrayType(DOUBLE), ImmutableList.of());
        assertFunction("REPEAT(array[1], 0)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of());

        // illegal inputs
        assertInvalidFunction("REPEAT(2, -1)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REPEAT(1, 1000000)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REPEAT('loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongvarchar', 9999)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("REPEAT(array[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 9999)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as array(bigint))", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[1,2,3]", BOOLEAN, false);
        assertOperator(INDETERMINATE, "array[1,2,3,null]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array['test1', 'test2', 'test3', 'test4']", BOOLEAN, false);
        assertOperator(INDETERMINATE, "array['test1', 'test2', 'test3', null]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array['test1', null, 'test2', 'test3']", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[null, 'test1', 'test2', 'test3']", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[null, time '12:34:56', time '01:23:45']", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[null, timestamp '2016-01-02 12:34:56', timestamp '2016-12-23 01:23:45']", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[null]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[null, null, null]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[row(1), row(2), row(3)]", BOOLEAN, false);
        assertOperator(INDETERMINATE, "array[cast(row(1) as row(a bigint)), cast(null as row(a bigint)), cast(row(3) as row(a bigint))]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[cast(row(1) as row(a bigint)), cast(row(null) as row(a bigint)), cast(row(3) as row(a bigint))]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[map(array[2], array[-2]), map(array[1], array[-1])]", BOOLEAN, false);
        assertOperator(INDETERMINATE, "array[map(array[2], array[-2]), null]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[map(array[2], array[-2]), map(array[1], array[null])]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[array[1], array[2], array[3]]", BOOLEAN, false);
        assertOperator(INDETERMINATE, "array[array[1], array[null], array[3]]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[array[1], array[2], null]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[1E0, 2E0, null]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[1E0, 2E0, 3E0]", BOOLEAN, false);
        assertOperator(INDETERMINATE, "array[true, false, null]", BOOLEAN, true);
        assertOperator(INDETERMINATE, "array[true, false, true]", BOOLEAN, false);
    }

    @Test
    public void testSequence()
            throws ParseException
    {
        // defaults to a step of 1
        assertFunction("SEQUENCE(1, 5)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 4L, 5L));
        assertFunction("SEQUENCE(-10, -5)", new ArrayType(BIGINT), ImmutableList.of(-10L, -9L, -8L, -7L, -6L, -5L));
        assertFunction("SEQUENCE(-5, 2)", new ArrayType(BIGINT), ImmutableList.of(-5L, -4L, -3L, -2L, -1L, 0L, 1L, 2L));
        assertFunction("SEQUENCE(2, 2)", new ArrayType(BIGINT), ImmutableList.of(2L));
        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-04-14')",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-04-13"), sqlDate("2016-04-14")));

        // defaults to a step of -1
        assertFunction("SEQUENCE(5, 1)", new ArrayType(BIGINT), ImmutableList.of(5L, 4L, 3L, 2L, 1L));
        assertFunction("SEQUENCE(-5, -10)", new ArrayType(BIGINT), ImmutableList.of(-5L, -6L, -7L, -8L, -9L, -10L));
        assertFunction("SEQUENCE(2, -5)", new ArrayType(BIGINT), ImmutableList.of(2L, 1L, 0L, -1L, -2L, -3L, -4L, -5L));
        assertFunction(
                "SEQUENCE(date '2016-04-14', date '2016-04-12')",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-14"), sqlDate("2016-04-13"), sqlDate("2016-04-12")));

        // with increment
        assertFunction("SEQUENCE(1, 9, 4)", new ArrayType(BIGINT), ImmutableList.of(1L, 5L, 9L));
        assertFunction("SEQUENCE(-10, -5, 2)", new ArrayType(BIGINT), ImmutableList.of(-10L, -8L, -6L));
        assertFunction("SEQUENCE(-5, 2, 3)", new ArrayType(BIGINT), ImmutableList.of(-5L, -2L, 1L));
        assertFunction("SEQUENCE(2, 2, 2)", new ArrayType(BIGINT), ImmutableList.of(2L));
        assertFunction("SEQUENCE(5, 1, -1)", new ArrayType(BIGINT), ImmutableList.of(5L, 4L, 3L, 2L, 1L));
        assertFunction("SEQUENCE(10, 2, -2)", new ArrayType(BIGINT), ImmutableList.of(10L, 8L, 6L, 4L, 2L));

        // failure modes
        assertInvalidFunction(
                "SEQUENCE(2, -1, 1)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(-1, -10, 1)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(1, 1000000)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
        assertInvalidFunction(
                "SEQUENCE(date '2000-04-14', date '2030-04-12')",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testSequenceDateTimeDayToSecond()
            throws ParseException
    {
        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-04-14', interval '1' day)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-04-13"), sqlDate("2016-04-14")));
        assertFunction(
                "SEQUENCE(date '2016-04-14', date '2016-04-12', interval '-1' day)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-14"), sqlDate("2016-04-13"), sqlDate("2016-04-12")));

        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-04-16', interval '2' day)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-04-14"), sqlDate("2016-04-16")));
        assertFunction(
                "SEQUENCE(date '2016-04-16', date '2016-04-12', interval '-2' day)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-16"), sqlDate("2016-04-14"), sqlDate("2016-04-12")));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:07:00', interval '3' minute)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 1, 3, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 1, 6, 10, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-04-16 01:03:00', interval '-3' minute)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 10, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 1, 7, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 1, 4, 10, 0, UTC, UTC_KEY, TEST_SESSION)));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '20' second)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 1, 0, 30, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 1, 0, 50, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:01:10', timestamp '2016-04-16 01:00:20', interval '-20' second)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 1, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 1, 0, 50, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 1, 0, 30, 0, UTC, UTC_KEY, TEST_SESSION)));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-18 01:01:00', interval '19' hour)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 16, 20, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 17, 15, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-14 01:00:20', interval '-19' hour)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 15, 6, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 4, 14, 11, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION)));

        // failure modes
        assertInvalidFunction(
                "SEQUENCE(date '2016-04-12', date '2016-04-14', interval '-1' day)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(date '2016-04-14', date '2016-04-12', interval '1' day)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(date '2000-04-14', date '2030-04-12', interval '1' day)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
        assertInvalidFunction(
                "SEQUENCE(date '2018-01-01', date '2018-01-04', interval '18' hour)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence step must be a day interval if start and end values are dates");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '-20' second)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-04-16 01:01:00', interval '20' second)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 09:01:00', interval '1' second)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testSequenceDateTimeYearToMonth()
            throws ParseException
    {
        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-06-12', interval '1' month)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-05-12"), sqlDate("2016-06-12")));
        assertFunction(
                "SEQUENCE(date '2016-06-12', date '2016-04-12', interval '-1' month)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-06-12"), sqlDate("2016-05-12"), sqlDate("2016-04-12")));

        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-08-12', interval '2' month)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-06-12"), sqlDate("2016-08-12")));
        assertFunction(
                "SEQUENCE(date '2016-08-12', date '2016-04-12', interval '-2' month)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-08-12"), sqlDate("2016-06-12"), sqlDate("2016-04-12")));

        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2018-04-12', interval '1' year)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2017-04-12"), sqlDate("2018-04-12")));
        assertFunction(
                "SEQUENCE(date '2018-04-12', date '2016-04-12', interval '-1' year)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2018-04-12"), sqlDate("2017-04-12"), sqlDate("2016-04-12")));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-09-16 01:10:00', interval '2' month)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 6, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 8, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction(
                "SEQUENCE(timestamp '2016-09-16 01:10:10', timestamp '2016-04-16 01:00:00', interval '-2' month)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 9, 16, 1, 10, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 7, 16, 1, 10, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2016, 5, 16, 1, 10, 10, 0, UTC, UTC_KEY, TEST_SESSION)));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2021-04-16 01:01:00', interval '2' year)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2018, 4, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2020, 4, 16, 1, 0, 10, 0, UTC, UTC_KEY, TEST_SESSION)));
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:01:10', timestamp '2011-04-16 01:00:00', interval '-2' year)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(
                        sqlTimestampOf(2016, 4, 16, 1, 1, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2014, 4, 16, 1, 1, 10, 0, UTC, UTC_KEY, TEST_SESSION),
                        sqlTimestampOf(2012, 4, 16, 1, 1, 10, 0, UTC, UTC_KEY, TEST_SESSION)));

        // failure modes
        assertInvalidFunction(
                "SEQUENCE(date '2016-06-12', date '2016-04-12', interval '1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(date '2016-04-12', date '2016-06-12', interval '-1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(date '2000-04-12', date '3000-06-12', interval '1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-05-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-05-16 01:01:00', interval '-1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '3000-04-16 09:01:00', interval '1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
    }

    @Override
    public void assertInvalidFunction(String projection, SemanticErrorCode errorCode)
    {
        try {
            assertFunction(projection, UNKNOWN, null);
            fail("Expected error " + errorCode + " from " + projection);
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), errorCode);
        }
    }

    @Test
    public void testFlatten()
    {
        // BOOLEAN Tests
        assertFunction("flatten(ARRAY [ARRAY [TRUE, FALSE], ARRAY [FALSE]])", new ArrayType(BOOLEAN), ImmutableList.of(true, false, false));
        assertFunction("flatten(ARRAY [ARRAY [TRUE, FALSE], NULL])", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("flatten(ARRAY [ARRAY [TRUE, FALSE]])", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("flatten(ARRAY [NULL, ARRAY [TRUE, FALSE]])", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("flatten(ARRAY [ARRAY [TRUE], ARRAY [FALSE], ARRAY [TRUE, FALSE]])", new ArrayType(BOOLEAN), ImmutableList.of(true, false, true, false));
        assertFunction("flatten(ARRAY [NULL, ARRAY [TRUE], NULL, ARRAY [FALSE], ARRAY [FALSE, TRUE]])", new ArrayType(BOOLEAN), ImmutableList.of(true, false, false, true));

        // VARCHAR Tests
        assertFunction("flatten(ARRAY [ARRAY ['1', '2'], ARRAY ['3']])", new ArrayType(createVarcharType(1)), ImmutableList.of("1", "2", "3"));
        assertFunction("flatten(ARRAY [ARRAY ['1', '2'], NULL])", new ArrayType(createVarcharType(1)), ImmutableList.of("1", "2"));
        assertFunction("flatten(ARRAY [NULL, ARRAY ['1', '2']])", new ArrayType(createVarcharType(1)), ImmutableList.of("1", "2"));
        assertFunction("flatten(ARRAY [ARRAY ['0'], ARRAY ['1'], ARRAY ['2', '3']])", new ArrayType(createVarcharType(1)), ImmutableList.of("0", "1", "2", "3"));
        assertFunction("flatten(ARRAY [NULL, ARRAY ['0'], NULL, ARRAY ['1'], ARRAY ['2', '3']])", new ArrayType(createVarcharType(1)), ImmutableList.of("0", "1", "2", "3"));

        // BIGINT Tests
        assertFunction("flatten(ARRAY [ARRAY [1, 2], ARRAY [3]])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("flatten(ARRAY [ARRAY [1, 2], NULL])", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("flatten(ARRAY [NULL, ARRAY [1, 2]])", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("flatten(ARRAY [ARRAY [0], ARRAY [1], ARRAY [2, 3]])", new ArrayType(INTEGER), ImmutableList.of(0, 1, 2, 3));
        assertFunction("flatten(ARRAY [NULL, ARRAY [0], NULL, ARRAY [1], ARRAY [2, 3]])", new ArrayType(INTEGER), ImmutableList.of(0, 1, 2, 3));

        // DOUBLE Tests
        assertFunction("flatten(ARRAY [ARRAY [1.2E0, 2.2E0], ARRAY [3.2E0]])", new ArrayType(DOUBLE), ImmutableList.of(1.2, 2.2, 3.2));
        assertFunction("flatten(ARRAY [ARRAY [1.2E0, 2.2E0], NULL])", new ArrayType(DOUBLE), ImmutableList.of(1.2, 2.2));
        assertFunction("flatten(ARRAY [NULL, ARRAY [1.2E0, 2.2E0]])", new ArrayType(DOUBLE), ImmutableList.of(1.2, 2.2));
        assertFunction("flatten(ARRAY [ARRAY[0.2E0], ARRAY [1.2E0], ARRAY [2.2E0, 3.2E0]])", new ArrayType(DOUBLE), ImmutableList.of(0.2, 1.2, 2.2, 3.2));
        assertFunction("flatten(ARRAY [NULL, ARRAY [0.2E0], NULL, ARRAY [1.2E0], ARRAY [2.2E0, 3.2E0]])", new ArrayType(DOUBLE), ImmutableList.of(0.2, 1.2, 2.2, 3.2));

        // ARRAY<BIGINT> tests
        assertFunction("flatten(ARRAY [ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [ARRAY [5, 6], ARRAY [7, 8]]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3, 4), ImmutableList.of(5, 6), ImmutableList.of(7, 8)));
        assertFunction("flatten(ARRAY [ARRAY [ARRAY [1, 2], ARRAY [3, 4]], NULL])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3, 4)));
        assertFunction("flatten(ARRAY [NULL, ARRAY [ARRAY [5, 6], ARRAY [7, 8]]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(5, 6), ImmutableList.of(7, 8)));

        // MAP<BIGINT, BIGINT> Tests
        assertFunction("flatten(ARRAY [ARRAY [MAP (ARRAY [1, 2], ARRAY [1, 2])], ARRAY [MAP (ARRAY [3, 4], ARRAY [3, 4])]])", new ArrayType(mapType(INTEGER, INTEGER)), ImmutableList.of(ImmutableMap.of(1, 1, 2, 2), ImmutableMap.of(3, 3, 4, 4)));
        assertFunction("flatten(ARRAY [ARRAY [MAP (ARRAY [1, 2], ARRAY [1, 2])], NULL])", new ArrayType(mapType(INTEGER, INTEGER)), ImmutableList.of(ImmutableMap.of(1, 1, 2, 2)));
        assertFunction("flatten(ARRAY [NULL, ARRAY [MAP (ARRAY [3, 4], ARRAY [3, 4])]])", new ArrayType(mapType(INTEGER, INTEGER)), ImmutableList.of(ImmutableMap.of(3, 3, 4, 4)));
    }

    @Test
    public void testArrayHashOperator()
    {
        assertArrayHashOperator("ARRAY[1, 2]", INTEGER, ImmutableList.of(1, 2));
        assertArrayHashOperator("ARRAY[true, false]", BOOLEAN, ImmutableList.of(true, false));

        // test with ARRAY[ MAP( ARRAY[1], ARRAY[2] ) ]
        MapType mapType = mapType(INTEGER, INTEGER);
        assertArrayHashOperator("ARRAY[MAP(ARRAY[1], ARRAY[2])]", mapType, ImmutableList.of(mapBlockOf(INTEGER, INTEGER, ImmutableMap.of(1L, 2L))));
    }

    public void assertInvalidFunction(String projection, ErrorCode errorCode)
    {
        try {
            assertFunction(projection, UNKNOWN, null);
            fail("Expected error " + errorCode + " from " + projection);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), errorCode);
        }
    }

    private void assertArrayHashOperator(String inputArray, Type elementType, List<Object> elements)
    {
        ArrayType arrayType = new ArrayType(elementType);
        BlockBuilder arrayArrayBuilder = arrayType.createBlockBuilder(null, 1);
        BlockBuilder arrayBuilder = elementType.createBlockBuilder(null, elements.size());
        for (Object element : elements) {
            appendToBlockBuilder(elementType, element, arrayBuilder);
        }
        arrayType.writeObject(arrayArrayBuilder, arrayBuilder.build());

        assertOperator(HASH_CODE, inputArray, BIGINT, arrayType.hash(arrayArrayBuilder.build(), 0));
    }

    private static SqlDate sqlDate(String dateString)
            throws ParseException
    {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return new SqlDate(toIntExact(TimeUnit.MILLISECONDS.toDays(dateFormat.parse(dateString).getTime())));
    }
}
