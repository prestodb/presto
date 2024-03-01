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

import com.facebook.presto.common.type.ArrayType;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.operator.scalar.ArrayCombinationsFunction.combinationCount;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.math.LongMath.factorial;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestArrayCombinationsFunction
        extends AbstractTestFunctions
{
    @Test
    public void testCombinationCount()
    {
        for (int n = 0; n < 5; n++) {
            for (int k = 0; k <= n; k++) {
                assertEquals(combinationCount(n, k), factorial(n) / factorial(n - k) / factorial(k));
            }
        }

        assertEquals(combinationCount(42, 7), 26978328);
        assertEquals(combinationCount(100, 4), 3921225);
    }

    @Test
    public void testBasic()
    {
        assertFunction("combinations(ARRAY['bar', 'foo', 'baz', 'foo'], 0)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(ImmutableList.of()));
        assertFunction("combinations(ARRAY['bar', 'foo', 'baz', 'foo'], 1)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar"),
                ImmutableList.of("foo"),
                ImmutableList.of("baz"),
                ImmutableList.of("foo")));
        assertFunction("combinations(ARRAY['bar', 'foo', 'baz', 'foo'], 2)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo"),
                ImmutableList.of("bar", "baz"),
                ImmutableList.of("foo", "baz"),
                ImmutableList.of("bar", "foo"),
                ImmutableList.of("foo", "foo"),
                ImmutableList.of("baz", "foo")));
        assertFunction("combinations(ARRAY['bar', 'foo', 'baz', 'foo'], 3)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo", "baz"),
                ImmutableList.of("bar", "foo", "foo"),
                ImmutableList.of("bar", "baz", "foo"),
                ImmutableList.of("foo", "baz", "foo")));
        assertFunction("combinations(ARRAY['bar', 'foo', 'baz', 'foo'], 4)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo", "baz", "foo")));
        assertFunction("combinations(ARRAY['bar', 'foo', 'baz', 'foo'], 5)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of());
        assertFunction("combinations(ARRAY['a', 'bb', 'ccc', 'dddd'], 2)", new ArrayType(new ArrayType(createVarcharType(4))), ImmutableList.of(
                ImmutableList.of("a", "bb"),
                ImmutableList.of("a", "ccc"),
                ImmutableList.of("bb", "ccc"),
                ImmutableList.of("a", "dddd"),
                ImmutableList.of("bb", "dddd"),
                ImmutableList.of("ccc", "dddd")));
    }

    @Test
    public void testLimits()
    {
        assertInvalidFunction("combinations(sequence(1, 40), -1)", INVALID_FUNCTION_ARGUMENT, "combination size must not be negative: -1");
        assertInvalidFunction("combinations(sequence(1, 40), 10)", INVALID_FUNCTION_ARGUMENT, "combination size must not exceed 5: 10");
        assertInvalidFunction("combinations(sequence(1, 100), 5)", INVALID_FUNCTION_ARGUMENT, "combinations exceed max size");
    }

    @Test
    public void testCardinality()
    {
        for (int n = 0; n < 5; n++) {
            for (int k = 0; k <= n; k++) {
                String array = "ARRAY" + ContiguousSet.closedOpen(0, n).asList();
                assertFunction(format("cardinality(combinations(%s, %s))", array, k), BIGINT, factorial(n) / factorial(n - k) / factorial(k));
            }
        }
    }

    @Test
    public void testNull()
    {
        assertFunction("combinations(CAST(NULL AS array(bigint)), 2)", new ArrayType(new ArrayType(BIGINT)), null);

        assertFunction("combinations(ARRAY['foo', NULL, 'bar'], 2)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                asList("foo", null),
                asList("foo", "bar"),
                asList(null, "bar")));

        assertFunction("combinations(ARRAY [NULL, NULL, NULL], 2)", new ArrayType(new ArrayType(UNKNOWN)), ImmutableList.of(
                asList(null, null),
                asList(null, null),
                asList(null, null)));

        assertFunction("combinations(ARRAY [NULL, 3, NULL], 2)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(
                asList(null, 3),
                asList(null, null),
                asList(3, null)));
    }

    @Test
    public void testTypeCombinations()
    {
        assertFunction("combinations(ARRAY[1, 2, 3], 2)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(
                ImmutableList.of(1, 2),
                ImmutableList.of(1, 3),
                ImmutableList.of(2, 3)));
        assertFunction("combinations(ARRAY[1.1E0, 2.1E0, 3.1E0], 2)", new ArrayType(new ArrayType(DOUBLE)), ImmutableList.of(
                ImmutableList.of(1.1, 2.1),
                ImmutableList.of(1.1, 3.1),
                ImmutableList.of(2.1, 3.1)));
        assertFunction("combinations(ARRAY[true, false, true], 2)", new ArrayType(new ArrayType(BOOLEAN)), ImmutableList.of(
                ImmutableList.of(true, false),
                ImmutableList.of(true, true),
                ImmutableList.of(false, true)));

        assertFunction("combinations(ARRAY[ARRAY['A1', 'A2'], ARRAY['B1'], ARRAY['C1', 'C2']], 2)", new ArrayType(new ArrayType(new ArrayType(createVarcharType(2)))), ImmutableList.of(
                ImmutableList.of(ImmutableList.of("A1", "A2"), ImmutableList.of("B1")),
                ImmutableList.of(ImmutableList.of("A1", "A2"), ImmutableList.of("C1", "C2")),
                ImmutableList.of(ImmutableList.of("B1"), ImmutableList.of("C1", "C2"))));

        assertFunction("combinations(ARRAY['\u4FE1\u5FF5\u7231', '\u5E0C\u671B', '\u671B'], 2)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("\u4FE1\u5FF5\u7231", "\u5E0C\u671B"),
                ImmutableList.of("\u4FE1\u5FF5\u7231", "\u671B"),
                ImmutableList.of("\u5E0C\u671B", "\u671B")));

        assertFunction("combinations(ARRAY[], 2)", new ArrayType(new ArrayType(UNKNOWN)), ImmutableList.of());
        assertFunction("combinations(ARRAY[''], 2)", new ArrayType(new ArrayType(createVarcharType(0))), ImmutableList.of());
        assertFunction("combinations(ARRAY['', ''], 2)", new ArrayType(new ArrayType(createVarcharType(0))), ImmutableList.of(ImmutableList.of("", "")));
    }
}
