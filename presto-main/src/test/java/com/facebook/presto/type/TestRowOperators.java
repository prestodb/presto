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

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.operator.scalar.TestingRowConstructor;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionRegistry.mangleFieldAccessor;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestRowOperators
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
        functionAssertions.getMetadata().getFunctionRegistry().addFunctions(new FunctionListBuilder(functionAssertions.getMetadata().getTypeManager()).scalar(TestingRowConstructor.class).getFunctions());
    }
    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testRowTypeLookup()
            throws Exception
    {
        functionAssertions.getMetadata().getType(parseTypeSignature("row<bigint>('a')"));
        Type type = functionAssertions.getMetadata().getType(parseTypeSignature("row<bigint>('b')"));
        assertEquals(type.getTypeSignature().getLiteralParameters(), ImmutableList.of("b"));
    }

    @Test
    public void testRowToJson()
            throws Exception
    {
        assertFunction("CAST(test_row(1, 2) AS JSON)", "[1,2]");
        assertFunction("CAST(test_row(1, CAST(NULL AS BIGINT)) AS JSON)", "[1,null]");
        assertFunction("CAST(test_row(1, 2.0) AS JSON)", "[1,2.0]");
        assertFunction("CAST(test_row(1.0, 2.5) AS JSON)", "[1.0,2.5]");
        assertFunction("CAST(test_row(1.0, 'kittens') AS JSON)", "[1.0,\"kittens\"]");
        assertFunction("CAST(test_row(TRUE, FALSE) AS JSON)", "[true,false]");
        assertFunction("CAST(test_row(from_unixtime(1)) AS JSON)", "[\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()).toString() + "\"]");
    }

    @Test
    public void testFieldAccessor()
            throws Exception
    {
        String mangledName1 = mangleFieldAccessor("col0");
        String mangledName2 = mangleFieldAccessor("col1");
        assertFunction('"' + mangledName1 + "\"(test_row(1, 2))", 1);
        assertFunction('"' + mangledName2 + "\"(test_row(1, 'kittens'))", "kittens");
    }

    @Test
    public void testRowEquality()
            throws Exception
    {
        assertFunction("test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10') = " +
                "test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')", true);
        assertFunction("test_row(1.0, test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')) =" +
                "test_row(1.0, test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10'))", true);
        assertFunction("test_row(1.0, 'kittens') = test_row(1.0, 'kittens')", true);
        assertFunction("test_row(1, 2.0) = test_row(1, 2.0)", true);
        assertFunction("test_row(TRUE, FALSE, TRUE, FALSE) = test_row(TRUE, FALSE, TRUE, FALSE)", true);
        assertFunction("test_row(TRUE, FALSE, TRUE, FALSE) = test_row(TRUE, TRUE, TRUE, FALSE)", false);
        assertFunction("test_row(1, 2.0, TRUE, 'kittens', from_unixtime(1)) = test_row(1, 2.0, TRUE, 'kittens', from_unixtime(1))", true);

        assertFunction("test_row(1.0, test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')) !=" +
                "test_row(1.0, test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:11'))", true);
        assertFunction("test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10') != " +
                "test_row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:11')", true);
        assertFunction("test_row(1.0, 'kittens') != test_row(1.0, 'kittens')", false);
        assertFunction("test_row(1, 2.0) != test_row(1, 2.0)", false);
        assertFunction("test_row(TRUE, FALSE, TRUE, FALSE) != test_row(TRUE, FALSE, TRUE, FALSE)", false);
        assertFunction("test_row(TRUE, FALSE, TRUE, FALSE) != test_row(TRUE, TRUE, TRUE, FALSE)", true);
        assertFunction("test_row(1, 2.0, TRUE, 'kittens', from_unixtime(1)) != test_row(1, 2.0, TRUE, 'puppies', from_unixtime(1))", true);

        try {
            assertFunction("test_row(cast(cast ('' as varbinary) as hyperloglog)) = test_row(cast(cast ('' as varbinary) as hyperloglog))", true);
            fail("hyperloglog is not comparable");
        }
        catch (RuntimeException e) {
            //Expected
        }

        try {
            assertFunction("test_row(TRUE, ARRAY [1], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) = test_row(TRUE, ARRAY [1,2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0]))", false);
            fail("map is not comparable");
        }
        catch (RuntimeException e) {
            //Expected
        }

        try {
            assertFunction("test_row(1, CAST(NULL AS BIGINT)) = test_row(1, 2)", false);
            fail("ROW comparison not implemented for NULL values");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode().getCode(), StandardErrorCode.NOT_SUPPORTED.toErrorCode().getCode());
        }

        assertFunction("test_row(TRUE, ARRAY [1]) = test_row(TRUE, ARRAY [1])", true);
        assertFunction("test_row(TRUE, ARRAY [1]) = test_row(TRUE, ARRAY [1,2])", false);
        assertFunction("test_row(1.0, ARRAY [1,2,3], test_row(2,2.0)) = test_row(1.0, ARRAY [1,2,3], test_row(2,2.0))", true);

        assertFunction("test_row(TRUE, ARRAY [1]) != test_row(TRUE, ARRAY [1])", false);
        assertFunction("test_row(TRUE, ARRAY [1]) != test_row(TRUE, ARRAY [1,2])", true);
        assertFunction("test_row(1.0, ARRAY [1,2,3], test_row(2,2.0)) != test_row(1.0, ARRAY [1,2,3], test_row(1,2.0))", true);
    }
}
