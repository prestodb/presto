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
import com.facebook.presto.spi.type.SqlTimestamp;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonType.JSON;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestJsonOperators
        extends AbstractTestFunctions
{
    // Some of the tests in this class are expected to fail when coercion between primitive presto types changes behavior

    @Test
    public void testCastToBigint()
    {
        assertFunction("cast(JSON 'null' as BIGINT)", BIGINT, null);
        assertFunction("cast(JSON '128' as BIGINT)", BIGINT, 128L);
        assertInvalidFunction("cast(JSON '12345678901234567890' as BIGINT)", INVALID_CAST_ARGUMENT);
        assertFunction("cast(JSON '128.9' as BIGINT)", BIGINT, 129L);
        assertFunction("cast(JSON '1234567890123456789.0' as BIGINT)", BIGINT, 1234567890123456768L); // loss of precision
        assertFunction("cast(JSON '12345678901234567890.0' as BIGINT)", BIGINT, 9223372036854775807L); // overflow. unexpected behavior. coherent with rest of Presto.
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
    public void testTypeConstructor()
            throws Exception
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
        assertFunction("cast(128 as JSON)", JSON, "128");
        assertFunction("cast(BIGINT '128' as JSON)", JSON, "128");
    }

    @Test
    public void testCastToDouble()
            throws Exception
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
            throws Exception
    {
        assertFunction("cast(cast (null as double) as JSON)", JSON, null);
        assertFunction("cast(3.14 as JSON)", JSON, "3.14");
        assertFunction("cast(nan() as JSON)", JSON, "\"NaN\"");
        assertFunction("cast(infinity() as JSON)", JSON, "\"Infinity\"");
        assertFunction("cast(-infinity() as JSON)", JSON, "\"-Infinity\"");
    }

    @Test
    public void testCastToDecimal()
            throws Exception
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
            throws Exception
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
        assertFunction("CAST(from_unixtime(1) AS JSON)", JSON, "\"" + sqlTimestamp(1000).toString() + "\"");
    }

    private static SqlTimestamp sqlTimestamp(long millisUtc)
    {
        return new SqlTimestamp(millisUtc, TEST_SESSION.getTimeZoneKey());
    }
}
