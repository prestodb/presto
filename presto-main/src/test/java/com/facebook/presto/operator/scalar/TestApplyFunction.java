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

package com.facebook.presto.operator.scalar;

import com.facebook.presto.Session;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestApplyFunction
        extends AbstractTestFunctions
{
    public TestApplyFunction()
    {
        this(testSessionBuilder().setTimeZoneKey(getTimeZoneKey("Pacific/Kiritimati")).build());
    }

    private TestApplyFunction(Session session)
    {
        super(session);
        functionAssertions.getMetadata().addFunctions(ImmutableList.of(APPLY_FUNCTION));
    }

    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction("apply(5, x -> x + 1)", INTEGER, 6);
        assertFunction("apply(5 + RANDOM(1), x -> x + 1)", INTEGER, 6);
    }

    @Test
    public void testNull()
            throws Exception
    {
        assertFunction("apply(3, x -> x + 1)", INTEGER, 4);
        assertFunction("apply(NULL, x -> x + 1)", INTEGER, null);
        assertFunction("apply(CAST (NULL AS INTEGER), x -> x + 1)", INTEGER, null);

        assertFunction("apply(3, x -> x IS NULL)", BOOLEAN, false);
        assertFunction("apply(NULL, x -> x IS NULL)", BOOLEAN, true);
        assertFunction("apply(CAST (NULL AS INTEGER), x -> x IS NULL)", BOOLEAN, true);
    }

    @Test
    public void testUnreferencedLambdaArgument()
    {
        assertFunction("apply(5, x -> 6)", INTEGER, 6);
    }

    @Test
    public void testSessionDependent()
            throws Exception
    {
        assertFunction("apply('timezone: ', x -> x || current_timezone())", VARCHAR, "timezone: Pacific/Kiritimati");
    }

    @Test
    public void testInstanceFunction()
    {
        assertFunction("apply(ARRAY[2], x -> concat(ARRAY [1], x))", new ArrayType(INTEGER), ImmutableList.of(1, 2));
    }

    @Test
    public void testWithTry()
            throws Exception
    {
        assertFunction("TRY(apply(5, x -> x + 1) / 0)", INTEGER, null);
        assertFunction("TRY(apply(5 + RANDOM(1), x -> x + 1) / 0)", INTEGER, null);
        assertInvalidFunction("apply(5 + RANDOM(1), x -> x + TRY(1 / 0))", NOT_SUPPORTED);
    }

    @Test
    public void testNestedLambda()
            throws Exception
    {
        assertFunction("apply(11, x -> apply(x + 7, y -> apply(y * 3, z -> z * 5) + 1) * 2)", INTEGER, 542);
        assertFunction("apply(11, x -> apply(x + 7, x -> apply(x * 3, x -> x * 5) + 1) * 2)", INTEGER, 542);
    }

    @Test
    public void testRowAccess()
            throws Exception
    {
        assertFunction("apply(CAST(ROW(1, 'a') AS ROW(x INTEGER, y VARCHAR)), r -> r.x)", INTEGER, 1);
        assertFunction("apply(CAST(ROW(1, 'a') AS ROW(x INTEGER, y VARCHAR)), r -> r.y)", VARCHAR, "a");
    }

    @Test
    public void testTypeCombinations()
            throws Exception
    {
        assertFunction("apply(25, x -> x + 1)", INTEGER, 26);
        assertFunction("apply(25, x -> x + 1.0)", DOUBLE, 26.0);
        assertFunction("apply(25, x -> x = 25)", BOOLEAN, true);
        assertFunction("apply(25, x -> to_base(x, 16))", createVarcharType(64), "19");
        assertFunction("apply(25, x -> ARRAY[x + 1])", new ArrayType(INTEGER), ImmutableList.of(26));

        assertFunction("apply(25.6, x -> CAST(x AS BIGINT))", BIGINT, 26L);
        assertFunction("apply(25.6, x -> x + 1.0)", DOUBLE, 26.6);
        assertFunction("apply(25.6, x -> x = 25.6)", BOOLEAN, true);
        assertFunction("apply(25.6, x -> CAST(x AS VARCHAR))", createUnboundedVarcharType(), "25.6");
        assertFunction("apply(25.6, x -> MAP(ARRAY[x + 1], ARRAY[true]))", new MapType(DOUBLE, BOOLEAN), ImmutableMap.of(26.6, true));

        assertFunction("apply(true, x -> if(x, 25, 26))", INTEGER, 25);
        assertFunction("apply(false, x -> if(x, 25.6, 28.9))", DOUBLE, 28.9);
        assertFunction("apply(true, x -> not x)", BOOLEAN, false);
        assertFunction("apply(false, x -> CAST(x AS VARCHAR))", createUnboundedVarcharType(), "false");
        assertFunction("apply(true, x -> ARRAY[x])", new ArrayType(BOOLEAN), ImmutableList.of(true));

        assertFunction("apply('41', x -> from_base(x, 16))", BIGINT, 65L);
        assertFunction("apply('25.6', x -> CAST(x AS DOUBLE))", DOUBLE, 25.6);
        assertFunction("apply('abc', x -> 'abc' = x)", BOOLEAN, true);
        assertFunction("apply('abc', x -> x || x)", createUnboundedVarcharType(), "abcabc");
        assertFunction(
                "apply('123', x -> ROW(x, CAST(x AS INTEGER), x > '0'))",
                new RowType(ImmutableList.of(createVarcharType(3), INTEGER, BOOLEAN), Optional.empty()),
                ImmutableList.of("123", 123, true));

        assertFunction("apply(ARRAY['abc', NULL, '123'], x -> from_base(x[3], 10))", BIGINT, 123L);
        assertFunction("apply(ARRAY['abc', NULL, '123'], x -> CAST(x[3] AS DOUBLE))", DOUBLE, 123.0);
        assertFunction("apply(ARRAY['abc', NULL, '123'], x -> x[2] IS NULL)", BOOLEAN, true);
        assertFunction("apply(ARRAY['abc', NULL, '123'], x -> x[2])", createVarcharType(3), null);
        assertFunction("apply(MAP(ARRAY['abc', 'def'], ARRAY[123, 456]), x -> map_keys(x))", new ArrayType(createVarcharType(3)), ImmutableList.of("abc", "def"));
    }
}
