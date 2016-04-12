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
import com.facebook.presto.operator.scalar.TestingRowConstructor;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonType.JSON;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestRowOperators
        extends AbstractTestFunctions
{
    public TestRowOperators()
    {
        registerScalar(TestingRowConstructor.class);
    }

    @Test
    public void testRowTypeLookup()
            throws Exception
    {
        functionAssertions.getMetadata().getType(parseTypeSignature("row(a bigint)"));
        Type type = functionAssertions.getMetadata().getType(parseTypeSignature("row(b bigint)"));
        assertEquals(type.getTypeSignature().getParameters().size(), 1);
        assertEquals(type.getTypeSignature().getParameters().get(0).getNamedTypeSignature().getName(), "b");
    }

    @Test
    public void testRowToJson()
            throws Exception
    {
        assertFunction("CAST(test_row(1, 2) AS JSON)", JSON, "[1,2]");
        assertFunction("CAST(test_row(1, CAST(NULL AS INTEGER)) AS JSON)", JSON, "[1,null]");
        assertFunction("CAST(test_row(1, 2.0) AS JSON)", JSON, "[1,2.0]");
        assertFunction("CAST(test_row(1.0, 2.5) AS JSON)", JSON, "[1.0,2.5]");
        assertFunction("CAST(test_row(1.0, 'kittens') AS JSON)", JSON, "[1.0,\"kittens\"]");
        assertFunction("CAST(test_row(TRUE, FALSE) AS JSON)", JSON, "[true,false]");
        assertFunction("CAST(test_row(from_unixtime(1)) AS JSON)", JSON, "[\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()) + "\"]");
        assertFunction("CAST(test_row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS JSON)", JSON, "[false,[1,2],{\"1\":2.0,\"3\":4.0}]");
    }

    @Test
    public void testFieldAccessor()
            throws Exception
    {
        // test_row has both (bigint, double) and (bigint, bigint) so this method is non-deterministic
        // assertFunction("test_row(1, NULL).col1", INTEGER,  null);
        // test_row has both (boolean, boolean) and (boolean, array<bigint>), so this method is non-deterministic
        // assertFunction("test_row(TRUE, NULL).col1", BOOLEAN, null);

        // test_row has both (boolean, boolean) and (boolean, array<bigint>) so this method is non-deterministic
        // assertFunction("test_row(TRUE, NULL).col1", BOOLEAN, null);

        assertFunction("test_row(1, CAST(NULL AS DOUBLE)).col1", DOUBLE, null);
        assertFunction("test_row(TRUE, CAST(NULL AS BOOLEAN)).col1", BOOLEAN, null);
        assertFunction("test_row(TRUE, CAST(NULL AS ARRAY<INTEGER>)).col1", new ArrayType(INTEGER), null);
        assertFunction("test_row(1.0, CAST(NULL AS VARCHAR)).col1", VARCHAR, null);
        assertFunction("test_row(1, 2).col0", INTEGER, 1);
        assertFunction("test_row(1, 'kittens').col1", VARCHAR, "kittens");
        assertFunction("test_row(1, 2).\"col1\"", INTEGER, 2);
        assertFunction("array[test_row(1, 2)][1].col1", INTEGER, 2);
        assertFunction("test_row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])).col1", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("test_row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])).col2", new MapType(INTEGER, DOUBLE), ImmutableMap.of(1, 2.0, 3, 4.0));
        assertFunction("test_row(1.0, ARRAY[test_row(31, 4.1), test_row(32, 4.2)], test_row(3, 4.0)).col1[2].col0", INTEGER, 32);
    }

    @Test
    public void testRowCast()
    {
        assertFunction("cast(test_row(2, 3) as row(aa bigint, bb bigint)).aa", BIGINT, 2L);
        assertFunction("cast(test_row(2, 3) as row(aa bigint, bb bigint)).bb", BIGINT, 3L);
        assertFunction("cast(test_row(2, 3) as row(aa bigint, bb boolean)).bb", BOOLEAN, true);
        assertFunction("cast(test_row(2, cast(null as double)) as row(aa bigint, bb double)).bb", DOUBLE, null);
        assertFunction("cast(test_row(2, 'test_str') as row(aa bigint, bb varchar)).bb", VARCHAR, "test_str");

        // there are totally 7 field names
        String longFieldNameCast = "CAST(test_row(1.2, ARRAY[test_row(233, 6.9)], test_row(1000, 6.3)) AS ROW(%s VARCHAR, %s ARRAY(ROW(%s VARCHAR, %s VARCHAR)), %s ROW(%s VARCHAR, %s VARCHAR))).%s[1].%s";
        int fieldCount = 7;
        char[] chars = new char[9333];
        String[] fields = new String[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            Arrays.fill(chars, (char) ('a' + i));
            fields[i] = new String(chars);
        }
        assertFunction(format(longFieldNameCast, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[1], fields[2]), VARCHAR, "233");
    }

    @Test
    public void testRowEquality()
            throws Exception
    {
        assertFunction("test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10') = " +
                "test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')", BOOLEAN, true);
        assertFunction("test_row(1.0, test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')) =" +
                "test_row(1.0, test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10'))", BOOLEAN, true);
        assertFunction("test_row(1.0, 'kittens') = test_row(1.0, 'kittens')", BOOLEAN, true);
        assertFunction("test_row(1, 2.0) = test_row(1, 2.0)", BOOLEAN, true);
        assertFunction("test_row(TRUE, FALSE, TRUE, FALSE) = test_row(TRUE, FALSE, TRUE, FALSE)", BOOLEAN, true);
        assertFunction("test_row(TRUE, FALSE, TRUE, FALSE) = test_row(TRUE, TRUE, TRUE, FALSE)", BOOLEAN, false);
        assertFunction("test_row(1, 2.0, TRUE, 'kittens', from_unixtime(1)) = test_row(1, 2.0, TRUE, 'kittens', from_unixtime(1))", BOOLEAN, true);

        assertFunction("test_row(1.0, test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')) !=" +
                "test_row(1.0, test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:11'))", BOOLEAN, true);
        assertFunction("test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10') != " +
                "test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:11')", BOOLEAN, true);
        assertFunction("test_row(1.0, 'kittens') != test_row(1.0, 'kittens')", BOOLEAN, false);
        assertFunction("test_row(1, 2.0) != test_row(1, 2.0)", BOOLEAN, false);
        assertFunction("test_row(TRUE, FALSE, TRUE, FALSE) != test_row(TRUE, FALSE, TRUE, FALSE)", BOOLEAN, false);
        assertFunction("test_row(TRUE, FALSE, TRUE, FALSE) != test_row(TRUE, TRUE, TRUE, FALSE)", BOOLEAN, true);
        assertFunction("test_row(1, 2.0, TRUE, 'kittens', from_unixtime(1)) != test_row(1, 2.0, TRUE, 'puppies', from_unixtime(1))", BOOLEAN, true);

        try {
            assertFunction("test_row(cast(cast ('' as varbinary) as hyperloglog)) = test_row(cast(cast ('' as varbinary) as hyperloglog))", BOOLEAN, true);
            fail("hyperloglog is not comparable");
        }
        catch (SemanticException e) {
            if (!e.getMessage().matches("\\Qline 1:55: '=' cannot be applied to row(col0 HyperLogLog), row(col0 HyperLogLog)\\E")) {
                throw e;
            }
            //Expected
        }

        assertFunction("test_row(TRUE, ARRAY [1], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) = test_row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0]))", BOOLEAN, false);
        assertFunction("test_row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) = test_row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0]))", BOOLEAN, true);

        try {
            assertFunction("test_row(1, CAST(NULL AS INTEGER)) = test_row(1, 2)", BOOLEAN, false);
            fail("ROW comparison not implemented for NULL values");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode().getCode(), StandardErrorCode.NOT_SUPPORTED.toErrorCode().getCode());
        }

        assertFunction("test_row(TRUE, ARRAY [1]) = test_row(TRUE, ARRAY [1])", BOOLEAN, true);
        assertFunction("test_row(TRUE, ARRAY [1]) = test_row(TRUE, ARRAY [1,2])", BOOLEAN, false);
        assertFunction("test_row(1.0, ARRAY [1,2,3], test_row(2,2.0)) = test_row(1.0, ARRAY [1,2,3], test_row(2,2.0))", BOOLEAN, true);

        assertFunction("test_row(TRUE, ARRAY [1]) != test_row(TRUE, ARRAY [1])", BOOLEAN, false);
        assertFunction("test_row(TRUE, ARRAY [1]) != test_row(TRUE, ARRAY [1,2])", BOOLEAN, true);
        assertFunction("test_row(1.0, ARRAY [1,2,3], test_row(2,2.0)) != test_row(1.0, ARRAY [1,2,3], test_row(1,2.0))", BOOLEAN, true);
    }
}
