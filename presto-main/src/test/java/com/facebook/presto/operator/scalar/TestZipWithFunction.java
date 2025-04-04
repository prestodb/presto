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
import com.facebook.presto.common.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;

public class TestZipWithFunction
        extends AbstractTestFunctions
{
    @Test
    public void testRetainedSizeBounded()
    {
        assertCachedInstanceHasBoundedRetainedSize("zip_with(ARRAY [25, 26, 27], ARRAY [1, 2, 3], (x, y) -> x + y)");
    }

    @Test
    public void testSameLength()
    {
        assertFunction("zip_with(ARRAY[], ARRAY[], (x, y) -> (y, x))",
                new ArrayType(RowType.anonymous(ImmutableList.of(UNKNOWN, UNKNOWN))),
                ImmutableList.of());

        assertFunction("zip_with(ARRAY[1, 2], ARRAY['a', 'b'], (x, y) -> (y, x))",
                new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(1), INTEGER))),
                ImmutableList.of(ImmutableList.of("a", 1), ImmutableList.of("b", 2)));

        assertFunction("zip_with(ARRAY[1, 2], ARRAY[CAST('a' AS VARCHAR), CAST('b' AS VARCHAR)], (x, y) -> (y, x))",
                new ArrayType(RowType.anonymous(ImmutableList.of(VARCHAR, INTEGER))),
                ImmutableList.of(ImmutableList.of("a", 1), ImmutableList.of("b", 2)));

        assertFunction("zip_with(ARRAY[1, 1], ARRAY[1, 2], (x, y) -> x + y)",
                new ArrayType(INTEGER),
                ImmutableList.of(2, 3));

        assertFunction("zip_with(CAST(ARRAY[3, 5] AS ARRAY(BIGINT)), CAST(ARRAY[1, 2] AS ARRAY(BIGINT)), (x, y) -> x * y)",
                new ArrayType(BIGINT),
                ImmutableList.of(3L, 10L));

        assertFunction("zip_with(ARRAY[true, false], ARRAY[false, true], (x, y) -> x OR y)",
                new ArrayType(BOOLEAN),
                ImmutableList.of(true, true));

        assertFunction("zip_with(ARRAY['a', 'b'], ARRAY['c', 'd'], (x, y) -> concat(x, y))",
                new ArrayType(VARCHAR),
                ImmutableList.of("ac", "bd"));

        assertFunction("zip_with(ARRAY[MAP(ARRAY[CAST ('a' AS VARCHAR)], ARRAY[1]), MAP(ARRAY[CAST('b' AS VARCHAR)], ARRAY[2])], ARRAY[MAP(ARRAY['c'], ARRAY[3]), MAP()], (x, y) -> map_concat(x, y))",
                new ArrayType(mapType(VARCHAR, INTEGER)),
                ImmutableList.of(ImmutableMap.of("a", 1, "c", 3), ImmutableMap.of("b", 2)));
    }

    @Test
    public void testDifferentLength()
    {
        assertFunction(
                "zip_with(ARRAY[1], ARRAY['a', 'bc'], (x, y) -> (y, x))",
                new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(2), INTEGER))),
                ImmutableList.of(ImmutableList.of("a", 1), asList("bc", null)));
        assertFunction(
                "zip_with(ARRAY[NULL, 2], ARRAY['a'], (x, y) -> (y, x))",
                new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(1), INTEGER))),
                ImmutableList.of(asList("a", null), asList(null, 2)));
        assertFunction(
                "zip_with(ARRAY[NULL, NULL], ARRAY[NULL, 2, 1], (x, y) -> x + y)",
                new ArrayType(INTEGER),
                asList(null, null, null));
    }

    @Test
    public void testWithNull()
    {
        assertFunction("zip_with(CAST(NULL AS ARRAY(UNKNOWN)), ARRAY[], (x, y) -> (y, x))",
                new ArrayType(RowType.anonymous(ImmutableList.of(UNKNOWN, UNKNOWN))),
                null);

        assertFunction("zip_with(ARRAY[NULL], ARRAY[NULL], (x, y) -> (y, x))",
                new ArrayType(RowType.anonymous(ImmutableList.of(UNKNOWN, UNKNOWN))),
                ImmutableList.of(asList(null, null)));

        assertFunction("zip_with(ARRAY[NULL], ARRAY[NULL], (x, y) -> x IS NULL AND y IS NULL)",
                new ArrayType(BOOLEAN),
                ImmutableList.of(true));

        assertFunction("zip_with(ARRAY['a', NULL], ARRAY[NULL, 1], (x, y) -> x IS NULL OR y IS NULL)",
                new ArrayType(BOOLEAN),
                ImmutableList.of(true, true));

        assertFunction("zip_with(ARRAY[1, NULL], ARRAY[3, 4], (x, y) -> x + y)",
                new ArrayType(INTEGER),
                asList(4, null));

        assertFunction("zip_with(ARRAY['a', 'b'], ARRAY[1, 3], (x, y) -> NULL)",
                new ArrayType(UNKNOWN),
                asList(null, null));
    }
}
