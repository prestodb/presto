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

import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.asList;

public class TestArrayTransformFunction
        extends AbstractTestFunctions
{
    public TestArrayTransformFunction()
    {
        super(testSessionBuilder().setTimeZoneKey(getTimeZoneKey("Pacific/Kiritimati")).build());
    }

    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction("transform(ARRAY [5, 6], x -> x + 1)", new ArrayType(INTEGER), ImmutableList.of(6, 7));
        assertFunction("transform(ARRAY [5 + RANDOM(1), 6], x -> x + 1)", new ArrayType(INTEGER), ImmutableList.of(6, 7));
    }

    @Test
    public void testNull()
            throws Exception
    {
        assertFunction("transform(ARRAY [3], x -> x + 1)", new ArrayType(INTEGER), ImmutableList.of(4));
        assertFunction("transform(ARRAY [NULL, NULL], x -> x + 1)", new ArrayType(INTEGER), asList(null, null));
        assertFunction("transform(ARRAY [NULL, 3, NULL], x -> x + 1)", new ArrayType(INTEGER), asList(null, 4, null));

        assertFunction("transform(ARRAY [3], x -> x IS NULL)", new ArrayType(BOOLEAN), ImmutableList.of(false));
        assertFunction("transform(ARRAY [NULL, NULL], x -> x IS NULL)", new ArrayType(BOOLEAN), ImmutableList.of(true, true));
        assertFunction("transform(ARRAY [NULL, 3, NULL], x -> x IS NULL)", new ArrayType(BOOLEAN), ImmutableList.of(true, false, true));
    }

    @Test
    public void testSessionDependent()
            throws Exception
    {
        assertFunction("transform(ARRAY['timezone: ', 'tz: '], x -> x || current_timezone())", new ArrayType(VARCHAR), ImmutableList.of("timezone: Pacific/Kiritimati", "tz: Pacific/Kiritimati"));
    }

    @Test
    public void testInstanceFunction()
    {
        assertFunction("transform(ARRAY[2, 3, 4, NULL, 5], x -> concat(ARRAY [1], x))", new ArrayType(new ArrayType(INTEGER)),
                asList(ImmutableList.of(1, 2), ImmutableList.of(1, 3), ImmutableList.of(1, 4), null, ImmutableList.of(1, 5)));
    }

    @Test
    public void testTypeCombinations()
            throws Exception
    {
        assertFunction("transform(ARRAY [25, 26], x -> x + 1)", new ArrayType(INTEGER), ImmutableList.of(26, 27));
        assertFunction("transform(ARRAY [25, 26], x -> x + 1.0)", new ArrayType(DOUBLE), ImmutableList.of(26.0, 27.0));
        assertFunction("transform(ARRAY [25, 26], x -> x = 25)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("transform(ARRAY [25, 26], x -> to_base(x, 16))", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("19", "1a"));
        assertFunction("transform(ARRAY [25, 26], x -> ARRAY[x + 1])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(26), ImmutableList.of(27)));

        assertFunction("transform(ARRAY [25.6, 27.3], x -> CAST(x AS BIGINT))", new ArrayType(BIGINT), ImmutableList.of(26L, 27L));
        assertFunction("transform(ARRAY [25.6, 27.3], x -> x + 1.0)", new ArrayType(DOUBLE), ImmutableList.of(26.6, 28.3));
        assertFunction("transform(ARRAY [25.6, 27.3], x -> x = 25.6)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("transform(ARRAY [25.6, 27.3], x -> CAST(x AS VARCHAR))", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("25.6", "27.3"));
        assertFunction(
                "transform(ARRAY [25.6, 27.3], x -> MAP(ARRAY[x + 1], ARRAY[true]))",
                new ArrayType(new MapType(DOUBLE, BOOLEAN)),
                ImmutableList.of(ImmutableMap.of(26.6, true), ImmutableMap.of(28.3, true)));

        assertFunction("transform(ARRAY [true, false], x -> if(x, 25, 26))", new ArrayType(INTEGER), ImmutableList.of(25, 26));
        assertFunction("transform(ARRAY [false, true], x -> if(x, 25.6, 28.9))", new ArrayType(DOUBLE), ImmutableList.of(28.9, 25.6));
        assertFunction("transform(ARRAY [true, false], x -> not x)", new ArrayType(BOOLEAN), ImmutableList.of(false, true));
        assertFunction("transform(ARRAY [false, true], x -> CAST(x AS VARCHAR))", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("false", "true"));
        assertFunction("transform(ARRAY [true, false], x -> ARRAY[x])", new ArrayType(new ArrayType(BOOLEAN)), ImmutableList.of(ImmutableList.of(true), ImmutableList.of(false)));

        assertFunction("transform(ARRAY ['41', '42'], x -> from_base(x, 16))", new ArrayType(BIGINT), ImmutableList.of(65L, 66L));
        assertFunction("transform(ARRAY ['25.6', '27.3'], x -> CAST(x AS DOUBLE))", new ArrayType(DOUBLE), ImmutableList.of(25.6, 27.3));
        assertFunction("transform(ARRAY ['abc', 'def'], x -> 'abc' = x)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("transform(ARRAY ['abc', 'def'], x -> x || x)", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("abcabc", "defdef"));
        assertFunction(
                "transform(ARRAY ['123', '456'], x -> ROW(x, CAST(x AS INTEGER), x > '3'))",
                new ArrayType(new RowType(ImmutableList.of(createVarcharType(3), INTEGER, BOOLEAN), Optional.empty())),
                ImmutableList.of(ImmutableList.of("123", 123, false), ImmutableList.of("456", 456, true)));

        assertFunction(
                "transform(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']], x -> from_base(x[3], 10))",
                new ArrayType(BIGINT),
                ImmutableList.of(123L, 456L));
        assertFunction(
                "transform(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']], x -> CAST(x[3] AS DOUBLE))",
                new ArrayType(DOUBLE),
                ImmutableList.of(123.0, 456.0));
        assertFunction(
                "transform(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']], x -> x[2] IS NULL)",
                new ArrayType(BOOLEAN),
                ImmutableList.of(true, false));
        assertFunction(
                "transform(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']], x -> x[2])",
                new ArrayType(createVarcharType(3)),
                asList(null, "x"));
        assertFunction(
                "transform(ARRAY [MAP(ARRAY['abc', 'def'], ARRAY[123, 456]), MAP(ARRAY['ghi', 'jkl'], ARRAY[234, 567])], x -> map_keys(x))",
                new ArrayType(new ArrayType(createVarcharType(3))),
                ImmutableList.of(ImmutableList.of("abc", "def"), ImmutableList.of("ghi", "jkl")));
    }
}
