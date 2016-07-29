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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestJsonOperators
        extends AbstractTestFunctions
{
    // Some of the tests in this class are expected to fail when coercion between primitive presto types changes behavior

    private LocalQueryRunner runner;

    @BeforeClass
    public void setUp()
    {
        runner = new LocalQueryRunner(TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (runner != null) {
            runner.close();
            runner = null;
        }
    }

    // todo add cases for decimal

    @Test
    public void testCastToBigint()
    {
        assertFunction("cast(JSON 'null' as BIGINT)", BIGINT, null);
        assertFunction("cast(JSON '128' as BIGINT)", BIGINT, 128L);
        assertInvalidFunction("cast(JSON '12345678901234567890' as BIGINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '128.9' as BIGINT)", BIGINT, 129L);
        assertFunction("cast(JSON '1234567890123456789.0' as BIGINT)", BIGINT, 1234567890123456768L); // loss of precision
        assertInvalidFunction("cast(JSON '12345678901234567890.0' as BIGINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '1e-324' as BIGINT)", BIGINT, 0L);
        assertInvalidFunction("cast(JSON '1e309' as BIGINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON 'true' as BIGINT)", BIGINT, 1L);
        assertFunction("cast(JSON 'false' as BIGINT)", BIGINT, 0L);
        assertFunction("cast(JSON '\"128\"' as BIGINT)", BIGINT, 128L);
        assertInvalidFunction("cast(JSON '\"12345678901234567890\"' as BIGINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"128.9\"' as BIGINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"true\"' as BIGINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"false\"' as BIGINT)", INVALID_CAST_ARGUMENT);

        assertFunction("cast(JSON ' 128' as BIGINT)", BIGINT, 128L); // leading space

        assertFunction("cast(json_extract('{\"x\":999}', '$.x') as BIGINT)", BIGINT, 999L);
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as BIGINT)");
    }

    @Test
    public void testCastToInteger()
    {
        assertFunction("cast(JSON 'null' as INTEGER)", INTEGER, null);
        assertFunction("cast(JSON '128' as INTEGER)", INTEGER, 128);
        assertInvalidFunction("cast(JSON '12345678901' as INTEGER)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '128.9' as INTEGER)", INTEGER, 129);
        assertInvalidFunction("cast(JSON '12345678901.0' as INTEGER)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '1e-324' as INTEGER)", INTEGER, 0);
        assertInvalidFunction("cast(JSON '1e309' as INTEGER)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON 'true' as INTEGER)", INTEGER, 1);
        assertFunction("cast(JSON 'false' as INTEGER)", INTEGER, 0);
        assertFunction("cast(JSON '\"128\"' as INTEGER)", INTEGER, 128);
        assertInvalidFunction("cast(JSON '\"12345678901234567890\"' as INTEGER)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"128.9\"' as INTEGER)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"true\"' as INTEGER)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"false\"' as INTEGER)", INVALID_CAST_ARGUMENT);

        assertFunction("cast(JSON ' 128' as INTEGER)", INTEGER, 128); // leading space

        assertFunction("cast(json_extract('{\"x\":999}', '$.x') as INTEGER)", INTEGER, 999);
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as INTEGER)");
    }

    @Test
    public void testCastToSmallint()
    {
        assertFunction("cast(JSON 'null' as SMALLINT)", SMALLINT, null);
        assertFunction("cast(JSON '128' as SMALLINT)", SMALLINT, (short) 128);
        assertInvalidFunction("cast(JSON '123456' as SMALLINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '128.9' as SMALLINT)", SMALLINT, (short) 129);
        assertInvalidFunction("cast(JSON '123456.0' as SMALLINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '1e-324' as SMALLINT)", SMALLINT, (short) 0);
        assertInvalidFunction("cast(JSON '1e309' as SMALLINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON 'true' as SMALLINT)", SMALLINT, (short) 1);
        assertFunction("cast(JSON 'false' as SMALLINT)", SMALLINT, (short) 0);
        assertFunction("cast(JSON '\"128\"' as SMALLINT)", SMALLINT, (short) 128);
        assertInvalidFunction("cast(JSON '\"123456\"' as SMALLINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"128.9\"' as SMALLINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"true\"' as SMALLINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"false\"' as SMALLINT)", INVALID_CAST_ARGUMENT);

        assertFunction("cast(JSON ' 128' as SMALLINT)", SMALLINT, (short) 128); // leading space

        assertFunction("cast(json_extract('{\"x\":999}', '$.x') as SMALLINT)", SMALLINT, (short) 999);
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as SMALLINT)");
    }

    @Test
    public void testCastToTinyint()
    {
        assertFunction("cast(JSON 'null' as TINYINT)", TINYINT, null);
        assertFunction("cast(JSON '12' as TINYINT)", TINYINT, (byte) 12);
        assertInvalidFunction("cast(JSON '1234' as TINYINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '12.9' as TINYINT)", TINYINT, (byte) 13);
        assertInvalidFunction("cast(JSON '1234.0' as TINYINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '1e-324' as TINYINT)", TINYINT, (byte) 0);
        assertInvalidFunction("cast(JSON '1e309' as TINYINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON 'true' as TINYINT)", TINYINT, (byte) 1);
        assertFunction("cast(JSON 'false' as TINYINT)", TINYINT, (byte) 0);
        assertFunction("cast(JSON '\"12\"' as TINYINT)", TINYINT, (byte) 12);
        assertInvalidFunction("cast(JSON '\"1234\"' as TINYINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"12.9\"' as TINYINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"true\"' as TINYINT)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"false\"' as TINYINT)", INVALID_CAST_ARGUMENT);

        assertFunction("cast(JSON ' 12' as TINYINT)", TINYINT, (byte) 12); // leading space

        assertFunction("cast(json_extract('{\"x\":99}', '$.x') as TINYINT)", TINYINT, (byte) 99);
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as TINYINT)");
    }

    @Test
    public void testTypeConstructor()
    {
        assertFunction("JSON '123'", JSON, "123");
        assertFunction("JSON '[4,5,6]'", JSON, "[4,5,6]");
        assertFunction("JSON '{ \"a\": 789 }'", JSON, "{\"a\":789}");
    }

    @Test
    public void testCastFromIntegrals()
    {
        assertFunction("cast(cast (null as integer) as JSON)", JSON, null);
        assertFunction("cast(cast (null as bigint) as JSON)", JSON, null);
        assertFunction("cast(cast (null as smallint) as JSON)", JSON, null);
        assertFunction("cast(cast (null as tinyint) as JSON)", JSON, null);
        assertFunction("cast(128 as JSON)", JSON, "128");
        assertFunction("cast(BIGINT '128' as JSON)", JSON, "128");
        assertFunction("cast(SMALLINT '128' as JSON)", JSON, "128");
        assertFunction("cast(TINYINT '127' as JSON)", JSON, "127");
    }

    @Test
    public void testCastToDouble()
    {
        assertFunction("cast(JSON 'null' as DOUBLE)", DOUBLE, null);
        assertFunction("cast(JSON '128' as DOUBLE)", DOUBLE, 128.0);
        assertFunction("cast(JSON '12345678901234567890' as DOUBLE)", DOUBLE, 1.2345678901234567e19);
        assertFunction("cast(JSON '128.9' as DOUBLE)", DOUBLE, 128.9);
        assertFunction("cast(JSON '1e-324' as DOUBLE)", DOUBLE, 0.0); // smaller than minimum subnormal positive
        assertFunction("cast(JSON '1e309' as DOUBLE)", DOUBLE, POSITIVE_INFINITY); // overflow
        assertFunction("cast(JSON '-1e309' as DOUBLE)", DOUBLE, NEGATIVE_INFINITY); // underflow
        assertFunction("cast(JSON 'true' as DOUBLE)", DOUBLE, 1.0);
        assertFunction("cast(JSON 'false' as DOUBLE)", DOUBLE, 0.0);
        assertFunction("cast(JSON '\"128\"' as DOUBLE)", DOUBLE, 128.0);
        assertFunction("cast(JSON '\"12345678901234567890\"' as DOUBLE)", DOUBLE, 1.2345678901234567e19);
        assertFunction("cast(JSON '\"128.9\"' as DOUBLE)", DOUBLE, 128.9);
        assertFunction("cast(JSON '\"NaN\"' as DOUBLE)", DOUBLE, Double.NaN);
        assertFunction("cast(JSON '\"Infinity\"' as DOUBLE)", DOUBLE, POSITIVE_INFINITY);
        assertFunction("cast(JSON '\"-Infinity\"' as DOUBLE)", DOUBLE, NEGATIVE_INFINITY);
        assertInvalidFunction("cast(JSON '\"true\"' as DOUBLE)", INVALID_CAST_ARGUMENT);

        assertFunction("cast(JSON ' 128.9' as DOUBLE)", DOUBLE, 128.9); // leading space

        assertFunction("cast(json_extract('{\"x\":1.23}', '$.x') as DOUBLE)", DOUBLE, 1.23);
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as DOUBLE)");
    }

    @Test
    public void testCastFromDouble()
    {
        assertFunction("cast(cast(null as double) as JSON)", JSON, null);
        assertFunction("cast(3.14E0 as JSON)", JSON, "3.14");
        assertFunction("cast(nan() as JSON)", JSON, "\"NaN\"");
        assertFunction("cast(infinity() as JSON)", JSON, "\"Infinity\"");
        assertFunction("cast(-infinity() as JSON)", JSON, "\"-Infinity\"");
    }

    @Test
    public void testCastFromReal()
    {
        assertFunction("cast(cast(null as REAL) as JSON)", JSON, null);
        assertFunction("cast(REAL '3.14' as JSON)", JSON, "3.14");
        assertFunction("cast(cast(nan() as REAL) as JSON)", JSON, "\"NaN\"");
        assertFunction("cast(cast(infinity() as REAL) as JSON)", JSON, "\"Infinity\"");
        assertFunction("cast(cast(-infinity() as REAL) as JSON)", JSON, "\"-Infinity\"");
    }

    @Test
    public void testCastToReal()
    {
        assertFunction("cast(JSON 'null' as REAL)", REAL, null);
        assertFunction("cast(JSON '-128' as REAL)", REAL, -128.0f);
        assertFunction("cast(JSON '128' as REAL)", REAL, 128.0f);
        assertFunction("cast(JSON '12345678901234567890' as REAL)", REAL, 1.2345679e19f);
        assertFunction("cast(JSON '128.9' as REAL)", REAL, 128.9f);
        assertFunction("cast(JSON '1e-46' as REAL)", REAL, 0.0f); // smaller than minimum subnormal positive
        assertFunction("cast(JSON '1e39' as REAL)", REAL, Float.POSITIVE_INFINITY); // overflow
        assertFunction("cast(JSON '-1e39' as REAL)", REAL, Float.NEGATIVE_INFINITY); // underflow
        assertFunction("cast(JSON 'true' as REAL)", REAL, 1.0f);
        assertFunction("cast(JSON 'false' as REAL)", REAL, 0.0f);
        assertFunction("cast(JSON '\"128\"' as REAL)", REAL, 128.0f);
        assertFunction("cast(JSON '\"12345678901234567890\"' as REAL)", REAL, 1.2345679e19f);
        assertFunction("cast(JSON '\"128.9\"' as REAL)", REAL, 128.9f);
        assertFunction("cast(JSON '\"NaN\"' as REAL)", REAL, Float.NaN);
        assertFunction("cast(JSON '\"Infinity\"' as REAL)", REAL, Float.POSITIVE_INFINITY);
        assertFunction("cast(JSON '\"-Infinity\"' as REAL)", REAL, Float.NEGATIVE_INFINITY);
        assertInvalidFunction("cast(JSON '\"true\"' as REAL)", INVALID_CAST_ARGUMENT);

        assertFunction("cast(JSON ' 128.9' as REAL)", REAL, 128.9f); // leading space

        assertFunction("cast(json_extract('{\"x\":1.23}', '$.x') as REAL)", REAL, 1.23f);
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as REAL)");
    }

    @Test
    public void testCastToDecimal()
    {
        assertFunction("cast(JSON 'null' as DECIMAL(10,3))", createDecimalType(10, 3), null);
        assertFunction("cast(JSON '128' as DECIMAL(10,3))", createDecimalType(10, 3), decimal("128.000"));
        assertFunction("cast(cast(DECIMAL '123456789012345678901234567890.12345678' as JSON) as DECIMAL(38,8))", createDecimalType(38, 8), decimal("123456789012345678901234567890.12345678"));
        assertFunction("cast(JSON '123.456' as DECIMAL(10,5))", createDecimalType(10, 5), decimal("123.45600"));
        assertFunction("cast(JSON 'true' as DECIMAL(10,5))", createDecimalType(10, 5), decimal("1.00000"));
        assertFunction("cast(JSON 'false' as DECIMAL(10,5))", createDecimalType(10, 5), decimal("0.00000"));
        assertInvalidCast("cast(JSON '1234567890123456' as DECIMAL(10,3))", "Cannot cast input json to DECIMAL(10,3)");
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as DECIMAL(10,3))", "Cannot cast '{\"x\":123}' to DECIMAL(10,3)");
        assertInvalidCast("cast(JSON '\"abc\"' as DECIMAL(10,3))", "Cannot cast '\"abc\"' to DECIMAL(10,3)");
    }

    @Test
    public void testCastFromDecimal()
    {
        assertFunction("cast(cast(null as decimal(5,2)) as JSON)", JSON, null);
        assertFunction("cast(DECIMAL '3.14' as JSON)", JSON, "3.14");
        assertFunction("cast(DECIMAL '12345678901234567890.123456789012345678' as JSON)", JSON, "12345678901234567890.123456789012345678");
    }

    @Test
    public void testCastToBoolean()
    {
        assertFunction("cast(JSON 'null' as BOOLEAN)", BOOLEAN, null);
        assertFunction("cast(JSON '0' as BOOLEAN)", BOOLEAN, false);
        assertFunction("cast(JSON '128' as BOOLEAN)", BOOLEAN, true);
        assertInvalidFunction("cast(JSON '12345678901234567890' as BOOLEAN)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '128.9' as BOOLEAN)", BOOLEAN, true);
        assertFunction("cast(JSON '1e-324' as BOOLEAN)", BOOLEAN, false); // smaller than minimum subnormal positive
        assertInvalidFunction("cast(JSON '1e309' as BOOLEAN)", INVALID_CAST_ARGUMENT); // overflow
        assertFunction("cast(JSON 'true' as BOOLEAN)", BOOLEAN, true);
        assertFunction("cast(JSON 'false' as BOOLEAN)", BOOLEAN, false);
        assertFunction("cast(JSON '\"True\"' as BOOLEAN)", BOOLEAN, true);
        assertFunction("cast(JSON '\"true\"' as BOOLEAN)", BOOLEAN, true);
        assertFunction("cast(JSON '\"false\"' as BOOLEAN)", BOOLEAN, false);
        assertInvalidFunction("cast(JSON '\"128\"' as BOOLEAN)", INVALID_CAST_ARGUMENT);
        assertInvalidFunction("cast(JSON '\"\"' as BOOLEAN)", INVALID_CAST_ARGUMENT);

        assertFunction("cast(JSON ' true' as BOOLEAN)", BOOLEAN, true); // leading space

        assertFunction("cast(json_extract('{\"x\":true}', '$.x') as BOOLEAN)", BOOLEAN, true);
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as BOOLEAN)");
    }

    @Test
    public void testCastFromBoolean()
    {
        assertFunction("cast(cast (null as boolean) as JSON)", JSON, null);
        assertFunction("cast(TRUE as JSON)", JSON, "true");
        assertFunction("cast(FALSE as JSON)", JSON, "false");
    }

    @Test
    public void testCastToVarchar()
    {
        assertFunction("cast(JSON 'null' as VARCHAR)", VARCHAR, null);
        assertFunction("cast(JSON '128' as VARCHAR)", VARCHAR, "128");
        assertFunction("cast(JSON '12345678901234567890' as VARCHAR)", VARCHAR, "12345678901234567890"); // overflow, no loss of precision
        assertFunction("cast(JSON '128.9' as VARCHAR)", VARCHAR, "128.9");
        assertFunction("cast(JSON '1e-324' as VARCHAR)", VARCHAR, "0.0"); // smaller than minimum subnormal positive
        assertFunction("cast(JSON '1e309' as VARCHAR)", VARCHAR, "Infinity"); // overflow
        assertFunction("cast(JSON '-1e309' as VARCHAR)", VARCHAR, "-Infinity"); // underflow
        assertFunction("cast(JSON 'true' as VARCHAR)", VARCHAR, "true");
        assertFunction("cast(JSON 'false' as VARCHAR)", VARCHAR, "false");
        assertFunction("cast(JSON '\"test\"' as VARCHAR)", VARCHAR, "test");
        assertFunction("cast(JSON '\"null\"' as VARCHAR)", VARCHAR, "null");
        assertFunction("cast(JSON '\"\"' as VARCHAR)", VARCHAR, "");

        assertFunction("cast(JSON ' \"test\"' as VARCHAR)", VARCHAR, "test"); // leading space

        assertFunction("cast(json_extract('{\"x\":\"y\"}', '$.x') as VARCHAR)", VARCHAR, "y");
        assertInvalidCast("cast(JSON '{ \"x\" : 123}' as VARCHAR)");
    }

    @Test
    public void testEquals()
    {
        assertFunction("json_parse('{ \"a\": \"1.1\" , \"b\": \"2.3\" , \"c\": { \"d\": \"314E-2\" }}') = json_parse('{ \"a\": \"1.1\" , \"b\": \"2.3\" , \"c\" : { \"d\" : \"314E-2\" }}')", BOOLEAN, true);
        assertFunction("JSON '[1,2,3]' = JSON '[1,2,3]'", BOOLEAN, true);
        assertFunction("JSON '{\"a\":1, \"b\":2}' = JSON '{\"b\":2, \"a\":1}'", BOOLEAN, true);
        assertFunction("JSON '{\"a\":1, \"b\":2}' = CAST(MAP(ARRAY['b','a'], ARRAY[2,1]) AS JSON)", BOOLEAN, true);
        assertFunction("JSON 'null' = JSON 'null'", BOOLEAN, true);
        assertFunction("JSON 'true' = JSON 'true'", BOOLEAN, true);
        assertFunction("JSON '{\"x\":\"y\"}' = JSON '{\"x\":\"y\"}'", BOOLEAN, true);
        assertFunction("JSON '[1,2,3]' = JSON '[2,3,1]'", BOOLEAN, false);
        assertFunction("JSON '{\"p_1\": 1, \"p_2\":\"v_2\", \"p_3\":null, \"p_4\":true, \"p_5\": {\"p_1\":1}}' = " +
                "JSON '{\"p_2\":\"v_2\", \"p_4\":true, \"p_1\": 1, \"p_3\":null, \"p_5\": {\"p_1\":1}}'", BOOLEAN, true);
    }

    @Test
    public void testNotEquals()
    {
        assertFunction("JSON '{ \"a\": 1 , \"b\": 2 , \"c\": { \"d\": 3 }}' != JSON '{ \"a\": 1 , \"b\": 2 , \"c\" : { \"d\" : 4 }}'", BOOLEAN, true);
        assertFunction("JSON '[1,2,3]' != JSON '[1,2,3]'", BOOLEAN, false);
        assertFunction("JSON '{\"a\":1, \"b\":2}' != JSON '{\"b\":2, \"a\":1}'", BOOLEAN, false);
        assertFunction("JSON 'null' != JSON 'null'", BOOLEAN, false);
        assertFunction("JSON 'true' != JSON 'true'", BOOLEAN, false);
        assertFunction("JSON '{\"x\":\"y\"}' != JSON '{\"x\":\"y\"}'", BOOLEAN, false);
        assertFunction("JSON '[1,2,3]' != JSON '[2,3,1]'", BOOLEAN, true);
        assertFunction("JSON '{\"p_1\": 1, \"p_2\":\"v_2\", \"p_3\":null, \"p_4\":true, \"p_5\": {\"p_1\":1}}' != " +
                "JSON '{\"p_2\":\"v_2\", \"p_4\":true, \"p_1\": 1, \"p_3\":null, \"p_5\": {\"p_1\":1}}'", BOOLEAN, false);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("JSON 'null' IS DISTINCT FROM JSON 'null'", BOOLEAN, false);
        assertFunction("JSON '{ \"a\": 1 , \"b\": 2 , \"c\": { \"d\": 3 }}' IS DISTINCT FROM JSON '{ \"a\": 1 , \"b\": 2 , \"c\" : { \"d\" : 4 }}'", BOOLEAN, true);
        assertFunction("JSON '{ \"a\": 1 , \"b\": 2 , \"c\": { \"d\": 3 }}' IS DISTINCT FROM JSON '{ \"b\": 2 , \"a\": 1 , \"c\": { \"d\": 3 }}'", BOOLEAN, false);
        assertFunction("JSON '{ \"a\": 1 , \"b\": 2 , \"c\": { \"d\": 3 }}' IS DISTINCT FROM JSON 'null'", BOOLEAN, true);
        assertFunction("JSON 'null' IS DISTINCT FROM JSON '{ \"a\": 1 , \"b\": 2 , \"c\" : { \"d\" : 4 }}'", BOOLEAN, true);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertFunction("cast(cast (null as varchar) as JSON)", JSON, null);
        assertFunction("cast('abc' as JSON)", JSON, "\"abc\"");
        assertFunction("cast('\"a\":2' as JSON)", JSON, "\"\\\"a\\\":2\"");
    }

    @Test
    public void testCastFromTimestamp()
    {
        assertFunction("cast(cast (null as timestamp) as JSON)", JSON, null);
        assertFunction("CAST(TIMESTAMP '1970-01-01 00:00:01' AS JSON)", JSON, format("\"%s\"", sqlTimestampOf(1970, 1, 1, 0, 0, 1, 0, UTC, UTC_KEY, TEST_SESSION)));
    }

    @Test
    public void testCastWithJsonParse()
    {
        // the test is to make sure ExpressionOptimizer works with cast + json_parse
        assertCastWithJsonParse("[[1,1], [2,2]]", "ARRAY<ARRAY<INTEGER>>", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 1), ImmutableList.of(2, 2)));
        assertInvalidCastWithJsonParse("[1, \"abc\"]", "ARRAY<INTEGER>", "Cannot cast to array(integer). Cannot cast 'abc' to INT\n[1, \"abc\"]");

        // Since we will not reformat the JSON string before parse and cast with the optimization,
        // these extra whitespaces in JSON string is to make sure the cast will work in such cases.
        assertCastWithJsonParse("{\"a\"\n:1,  \"b\":\t2}", "MAP<VARCHAR,INTEGER>", mapType(VARCHAR, INTEGER), ImmutableMap.of("a", 1, "b", 2));
        assertInvalidCastWithJsonParse("{\"[1, 1]\":[2, 2]}", "MAP<ARRAY<INTEGER>,ARRAY<INTEGER>>", "Cannot cast JSON to map(array(integer),array(integer))");
        assertInvalidCastWithJsonParse("{true: false, false:false}", "MAP<BOOLEAN,BOOLEAN>", "Cannot cast to map(boolean,boolean).\n{true: false, false:false}");

        assertCastWithJsonParse(
                "{\"a\"  \n  :1,  \"b\":  \t  [2, 3]}",
                "ROW(a INTEGER, b ARRAY<INTEGER>)",
                RowType.from(ImmutableList.of(
                        RowType.field("a", INTEGER),
                        RowType.field("b", new ArrayType(INTEGER)))),
                ImmutableList.of(1, ImmutableList.of(2, 3)));
        assertCastWithJsonParse(
                "[  1,  [2, 3]  ]",
                "ROW(INTEGER, ARRAY<INTEGER>)",
                RowType.anonymous(ImmutableList.of(INTEGER, new ArrayType(INTEGER))),
                ImmutableList.of(1, ImmutableList.of(2, 3)));
        assertInvalidCastWithJsonParse(
                "{\"a\" :1,  \"b\": {} }",
                "ROW(a INTEGER, b ARRAY<INTEGER>)",
                "Cannot cast to row(a integer,b array(integer)). Expected a json array, but got {\n{\"a\" :1,  \"b\": {} }");
        assertInvalidCastWithJsonParse(
                "[  1,  {}  ]",
                "ROW(INTEGER, ARRAY<INTEGER>)",
                "Cannot cast to row(integer,array(integer)). Expected a json array, but got {\n[  1,  {}  ]");
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as JSON)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "JSON '128'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON 'true'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON 'false'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '\"test\"'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '\"null\"'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '\"\"'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON 'true'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON 'false'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '\"True\"'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '\"true\"'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '123.456'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON 'true'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON 'false'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '\"NaN\"'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '\"Infinity\"'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "JSON '\"-Infinity\"'", BOOLEAN, false);
    }

    private void assertCastWithJsonParse(String json, String castSqlType, Type expectedType, Object expected)
    {
        String query = "" +
                "SELECT CAST(JSON_PARSE(col) AS " + castSqlType + ") " +
                "FROM (VALUES('" + json + "')) AS t(col)";

        // building queries with VALUES to avoid constant folding
        MaterializedResult result = runner.execute(query);
        assertEquals(result.getTypes().size(), 1);
        assertEquals(result.getTypes().get(0), expectedType);
        assertEquals(result.getOnlyValue(), expected);
    }

    private void assertInvalidCastWithJsonParse(String json, String castSqlType, String message)
    {
        String query = "" +
                "SELECT CAST(JSON_PARSE(col) AS " + castSqlType + ") " +
                "FROM (VALUES('" + json + "')) AS t(col)";

        try {
            runner.execute(query);
            fail("Expected to throw an INVALID_CAST_ARGUMENT exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_CAST_ARGUMENT.toErrorCode());
            assertEquals(e.getMessage(), message);
        }
    }
}
