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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.ArrayType;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.StructuralTestUtil.mapType;

public class TestMapTransformValueFunction
        extends AbstractTestFunctions
{
    @Test
    public void testRetainedSizeBounded()
    {
        assertCachedInstanceHasBoundedRetainedSize("transform_values(map(ARRAY [25, 26, 27], ARRAY [25, 26, 27]), (k, v) -> k + v)");
    }

    @Test
    public void testEmpty()
    {
        assertFunction("transform_values(map(ARRAY[], ARRAY[]), (k, v) -> NULL)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("transform_values(map(ARRAY[], ARRAY[]), (k, v) -> k)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("transform_values(map(ARRAY[], ARRAY[]), (k, v) -> v)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());

        assertFunction("transform_values(map(ARRAY[], ARRAY[]), (k, v) -> 0)", mapType(UNKNOWN, INTEGER), ImmutableMap.of());
        assertFunction("transform_values(map(ARRAY[], ARRAY[]), (k, v) -> true)", mapType(UNKNOWN, BOOLEAN), ImmutableMap.of());
        assertFunction("transform_values(map(ARRAY[], ARRAY[]), (k, v) -> 'value')", mapType(UNKNOWN, createVarcharType(5)), ImmutableMap.of());
        assertFunction("transform_values(CAST (map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> k + CAST(v as BIGINT))", mapType(BIGINT, BIGINT), ImmutableMap.of());
        assertFunction("transform_values(CAST (map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> CAST(k AS VARCHAR) || v)", mapType(BIGINT, VARCHAR), ImmutableMap.of());
    }

    @Test
    public void testNullValue()
    {
        Map<Integer, Void> sequenceToNullMap = new HashMap<>();
        sequenceToNullMap.put(1, null);
        sequenceToNullMap.put(2, null);
        sequenceToNullMap.put(3, null);
        assertFunction("transform_values(map(ARRAY[1, 2, 3], ARRAY ['a', 'b', 'c']), (k, v) -> NULL)", mapType(INTEGER, UNKNOWN), sequenceToNullMap);

        Map<Integer, String> mapWithNullValue = new HashMap<>();
        mapWithNullValue.put(1, "a");
        mapWithNullValue.put(2, "b");
        mapWithNullValue.put(3, null);
        assertFunction("transform_values(map(ARRAY[1, 2, 3], ARRAY ['a', 'b', NULL]), (k, v) -> v)", mapType(INTEGER, createVarcharType(1)), mapWithNullValue);
        assertFunction("transform_values(map(ARRAY[1, 2, 3], ARRAY [10, 11, NULL]), (k, v) -> to_base(v, 16))", mapType(INTEGER, createVarcharType(64)), mapWithNullValue);
        assertFunction("transform_values(map(ARRAY[1, 2, 3], ARRAY ['10', '11', 'Invalid']), (k, v) -> to_base(TRY_CAST(v as BIGINT), 16))", mapType(INTEGER, createVarcharType(64)), mapWithNullValue);
        assertFunction(
                "transform_values(map(ARRAY[1, 2, 3], ARRAY [0, 0, 0]), (k, v) -> element_at(map(ARRAY[1, 2], ARRAY['a', 'b']), k + v))",
                mapType(INTEGER, createVarcharType(1)),
                mapWithNullValue);

        assertFunction(
                "transform_values(map(ARRAY[1, 2, 3], ARRAY ['a', 'b', NULL]), (k, v) -> IF(v IS NULL, k + 1.0E0, k + 0.5E0))",
                mapType(INTEGER, DOUBLE),
                ImmutableMap.of(1, 1.5, 2, 2.5, 3, 4.0));
    }

    @Test
    public void testBasic()
    {
        assertFunction(
                "transform_values(map(ARRAY [1, 2, 3, 4], ARRAY [10, 20, 30, 40]), (k, v) -> k + v)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(1, 11, 2, 22, 3, 33, 4, 44));

        assertFunction(
                "transform_values(map(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, 2, 3, 4]), (k, v) -> v * v)",
                mapType(createVarcharType(1), INTEGER),
                ImmutableMap.of("a", 1, "b", 4, "c", 9, "d", 16));

        assertFunction(
                "transform_values(map(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, 2, 3, 4]), (k, v) -> k || CAST(v as VARCHAR))",
                mapType(createVarcharType(1), VARCHAR),
                ImmutableMap.of("a", "a1", "b", "b2", "c", "c3", "d", "d4"));

        assertFunction(
                "transform_values(map(ARRAY[1, 2, 3], ARRAY [1.0E0, 1.4E0, 1.7E0]), (k, v) -> map(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three'])[k] || '_' || CAST(v AS VARCHAR))",
                mapType(INTEGER, VARCHAR),
                ImmutableMap.of(1, "one_1.0", 2, "two_1.4", 3, "three_1.7"));
    }

    @Test
    public void testTypeCombinations()
    {
        assertFunction(
                "transform_values(map(ARRAY [25, 26, 27], ARRAY [25, 26, 27]), (k, v) -> k + v)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(25, 50, 26, 52, 27, 54));
        assertFunction(
                "transform_values(map(ARRAY [25, 26, 27], ARRAY [26.1E0, 31.2E0, 37.1E0]), (k, v) -> CAST(v - k AS BIGINT))",
                mapType(INTEGER, BIGINT),
                ImmutableMap.of(25, 1L, 26, 5L, 27, 10L));
        assertFunction(
                "transform_values(map(ARRAY [25, 27], ARRAY [false, true]), (k, v) -> if(v, k + 1, k + 2))",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(25, 27, 27, 28));
        assertFunction(
                "transform_values(map(ARRAY [25, 26, 27], ARRAY ['abc', 'd', 'xy']), (k, v) -> k + length(v))",
                mapType(INTEGER, BIGINT),
                ImmutableMap.of(25, 28L, 26, 27L, 27, 29L));
        assertFunction(
                "transform_values(map(ARRAY [25, 26, 27], ARRAY [ARRAY ['a'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k + cardinality(v))",
                mapType(INTEGER, BIGINT),
                ImmutableMap.of(25, 26L, 26, 28L, 27, 30L));

        assertFunction(
                "transform_values(map(ARRAY [25.5E0, 26.75E0, 27.875E0], ARRAY [25, 26, 27]), (k, v) -> k - v)",
                mapType(DOUBLE, DOUBLE),
                ImmutableMap.of(25.5, 0.5, 26.75, 0.75, 27.875, 0.875));
        assertFunction(
                "transform_values(map(ARRAY [25.5E0, 26.75E0, 27.875E0], ARRAY [25.0E0, 26.0E0, 27.0E0]), (k, v) -> k - v)",
                mapType(DOUBLE, DOUBLE),
                ImmutableMap.of(25.5, 0.5, 26.75, 0.75, 27.875, 0.875));
        assertFunction(
                "transform_values(map(ARRAY [25.5E0, 27.5E0], ARRAY [false, true]), (k, v) -> if(v, k + 0.1E0, k + 0.2E0))",
                mapType(DOUBLE, DOUBLE),
                ImmutableMap.of(25.5, 25.7, 27.5, 27.6));
        assertFunction(
                "transform_values(map(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY ['a', 'def', 'xy']), (k, v) -> k + length(v))",
                mapType(DOUBLE, DOUBLE),
                ImmutableMap.of(25.5, 26.5, 26.5, 29.5, 27.5, 29.5));
        assertFunction(
                "transform_values(map(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [ARRAY ['a'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k + cardinality(v))",
                mapType(DOUBLE, DOUBLE),
                ImmutableMap.of(25.5, 26.5, 26.5, 28.5, 27.5, 30.5));

        assertFunction(
                "transform_values(map(ARRAY [true, false], ARRAY [25, 26]), (k, v) -> k AND v = 25)",
                mapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(true, true, false, false));
        assertFunction(
                "transform_values(map(ARRAY [false, true], ARRAY [25.5E0, 26.5E0]), (k, v) -> k OR v > 100)",
                mapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(false, false, true, true));
        assertFunction(
                "transform_values(map(ARRAY [true, false], ARRAY [false, null]), (k, v) -> NOT k OR v)",
                mapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(false, true, true, false));
        assertFunction(
                "transform_values(map(ARRAY [false, true], ARRAY ['abc', 'def']), (k, v) -> NOT k AND v = 'abc')",
                mapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(false, true, true, false));
        assertFunction(
                "transform_values(map(ARRAY [true, false], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'b', 'c']]), (k, v) -> k OR cardinality(v) = 3)",
                mapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(false, true, true, true));

        assertFunction(
                "transform_values(map(ARRAY ['s0', 's1', 's2'], ARRAY [25, 26, 27]), (k, v) -> k || ':' || CAST(v as VARCHAR))",
                mapType(createVarcharType(2), VARCHAR),
                ImmutableMap.of("s0", "s0:25", "s1", "s1:26", "s2", "s2:27"));
        assertFunction(
                "transform_values(map(ARRAY ['s0', 's1', 's2'], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k || ':' || CAST(v as VARCHAR))",
                mapType(createVarcharType(2), VARCHAR),
                ImmutableMap.of("s0", "s0:25.5", "s1", "s1:26.5", "s2", "s2:27.5"));
        assertFunction(
                "transform_values(map(ARRAY ['s0', 's2'], ARRAY [false, true]), (k, v) -> if(v, k, CAST(v AS VARCHAR)))",
                mapType(createVarcharType(2), VARCHAR),
                ImmutableMap.of("s0", "false", "s2", "s2"));
        assertFunction(
                "transform_values(map(ARRAY ['s0', 's1', 's2'], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k || ':' || v)",
                mapType(createVarcharType(2), VARCHAR),
                ImmutableMap.of("s0", "s0:abc", "s1", "s1:def", "s2", "s2:xyz"));
        assertFunction(
                "transform_values(map(ARRAY ['s0', 's1', 's2'], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k || ':' || array_max(v))",
                mapType(createVarcharType(2), VARCHAR),
                ImmutableMap.of("s0", "s0:b", "s1", "s1:c", "s2", "s2:c"));

        assertFunction(
                "transform_values(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [25, 26]), (k, v) -> if(v % 2 = 0, reverse(k), k))",
                mapType(new ArrayType(INTEGER), new ArrayType(INTEGER)),
                ImmutableMap.of(ImmutableList.of(1, 2), ImmutableList.of(1, 2), ImmutableList.of(3, 4), ImmutableList.of(4, 3)));
        assertFunction(
                "transform_values(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [25.5E0, 26.5E0]), (k, v) -> CAST(k AS ARRAY(DOUBLE)) || v)",
                mapType(new ArrayType(INTEGER), new ArrayType(DOUBLE)),
                ImmutableMap.of(ImmutableList.of(1, 2), ImmutableList.of(1., 2., 25.5), ImmutableList.of(3, 4), ImmutableList.of(3., 4., 26.5)));
        assertFunction(
                "transform_values(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [false, true]), (k, v) -> if(v, reverse(k), k))",
                mapType(new ArrayType(INTEGER), new ArrayType(INTEGER)),
                ImmutableMap.of(ImmutableList.of(1, 2), ImmutableList.of(1, 2), ImmutableList.of(3, 4), ImmutableList.of(4, 3)));
        assertFunction(
                "transform_values(map(ARRAY [ARRAY [1, 2], ARRAY []], ARRAY ['a', 'ff']), (k, v) -> k || from_base(v, 16))",
                mapType(new ArrayType(INTEGER), new ArrayType(BIGINT)),
                ImmutableMap.of(ImmutableList.of(1, 2), ImmutableList.of(1L, 2L, 10L), ImmutableList.of(), ImmutableList.of(255L)));
        assertFunction(
                "transform_values(map(ARRAY [ARRAY [3, 4], ARRAY []], ARRAY [ARRAY ['a', 'b', 'c'], ARRAY ['a', 'c']]), (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v)",
                mapType(new ArrayType(INTEGER), new ArrayType(VARCHAR)),
                ImmutableMap.of(ImmutableList.of(3, 4), ImmutableList.of("3", "4", "a", "b", "c"), ImmutableList.of(), ImmutableList.of("a", "c")));
    }
}
