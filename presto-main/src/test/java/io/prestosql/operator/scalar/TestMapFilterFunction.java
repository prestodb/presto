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

public class TestMapFilterFunction
        extends AbstractTestFunctions
{
    @Test
    public void testRetainedSizeBounded()
    {
        assertCachedInstanceHasBoundedRetainedSize("map_filter(map(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, 2, NULL, 4]), (k, v) -> v IS NOT NULL)");
    }

    @Test
    public void testEmpty()
    {
        assertFunction("map_filter(map(ARRAY[], ARRAY[]), (k, v) -> true)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("map_filter(map(ARRAY[], ARRAY[]), (k, v) -> false)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("map_filter(map(ARRAY[], ARRAY[]), (k, v) -> CAST (NULL AS BOOLEAN))", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("map_filter(CAST (map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> true)", mapType(BIGINT, VARCHAR), ImmutableMap.of());
    }

    @Test
    public void testNull()
    {
        Map<Integer, Void> oneToNullMap = new HashMap<>();
        oneToNullMap.put(1, null);
        assertFunction("map_filter(map(ARRAY[1], ARRAY [NULL]), (k, v) -> v IS NULL)", mapType(INTEGER, UNKNOWN), oneToNullMap);
        assertFunction("map_filter(map(ARRAY[1], ARRAY [NULL]), (k, v) -> v IS NOT NULL)", mapType(INTEGER, UNKNOWN), ImmutableMap.of());
        assertFunction("map_filter(map(ARRAY[1], ARRAY [CAST (NULL AS INTEGER)]), (k, v) -> v IS NULL)", mapType(INTEGER, INTEGER), oneToNullMap);
        Map<Integer, Void> sequenceToNullMap = new HashMap<>();
        sequenceToNullMap.put(1, null);
        sequenceToNullMap.put(2, null);
        sequenceToNullMap.put(3, null);
        assertFunction("map_filter(map(ARRAY[1, 2, 3], ARRAY [NULL, NULL, NULL]), (k, v) -> v IS NULL)", mapType(INTEGER, UNKNOWN), sequenceToNullMap);
        assertFunction("map_filter(map(ARRAY[1, 2, 3], ARRAY [NULL, NULL, NULL]), (k, v) -> v IS NOT NULL)", mapType(INTEGER, UNKNOWN), ImmutableMap.of());
    }

    @Test
    public void testBasic()
    {
        assertFunction(
                "map_filter(map(ARRAY [5, 6, 7, 8], ARRAY [5, 6, 6, 5]), (x, y) -> x <= 6 OR y = 5)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(5, 5, 6, 6, 8, 5));
        assertFunction(
                "map_filter(map(ARRAY [5 + RANDOM(1), 6, 7, 8], ARRAY [5, 6, 6, 5]), (x, y) -> x <= 6 OR y = 5)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(5, 5, 6, 6, 8, 5));
        assertFunction(
                "map_filter(map(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, 2, NULL, 4]), (k, v) -> v IS NOT NULL)",
                mapType(createVarcharType(1), INTEGER),
                ImmutableMap.of("a", 1, "b", 2, "d", 4));
        assertFunction(
                "map_filter(map(ARRAY ['a', 'b', 'c'], ARRAY [TRUE, FALSE, NULL]), (k, v) -> v)",
                mapType(createVarcharType(1), BOOLEAN),
                ImmutableMap.of("a", true));
    }

    @Test
    public void testTypeCombinations()
    {
        assertFunction(
                "map_filter(map(ARRAY [25, 26, 27], ARRAY [25, 26, 27]), (k, v) -> k = 25 OR v = 27)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(25, 25, 27, 27));
        assertFunction(
                "map_filter(map(ARRAY [25, 26, 27], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k = 25 OR v = 27.5E0)",
                mapType(INTEGER, DOUBLE),
                ImmutableMap.of(25, 25.5, 27, 27.5));
        assertFunction(
                "map_filter(map(ARRAY [25, 26, 27], ARRAY [false, null, true]), (k, v) -> k = 25 OR v)",
                mapType(INTEGER, BOOLEAN),
                ImmutableMap.of(25, false, 27, true));
        assertFunction(
                "map_filter(map(ARRAY [25, 26, 27], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k = 25 OR v = 'xyz')",
                mapType(INTEGER, createVarcharType(3)),
                ImmutableMap.of(25, "abc", 27, "xyz"));
        assertFunction(
                "map_filter(map(ARRAY [25, 26, 27], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k = 25 OR cardinality(v) = 3)",
                mapType(INTEGER, new ArrayType(createVarcharType(1))),
                ImmutableMap.of(25, ImmutableList.of("a", "b"), 27, ImmutableList.of("a", "b", "c")));

        assertFunction(
                "map_filter(map(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [25, 26, 27]), (k, v) -> k = 25.5E0 OR v = 27)",
                mapType(DOUBLE, INTEGER),
                ImmutableMap.of(25.5, 25, 27.5, 27));
        assertFunction(
                "map_filter(map(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k = 25.5E0 OR v = 27.5E0)",
                mapType(DOUBLE, DOUBLE),
                ImmutableMap.of(25.5, 25.5, 27.5, 27.5));
        assertFunction(
                "map_filter(map(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [false, null, true]), (k, v) -> k = 25.5E0 OR v)",
                mapType(DOUBLE, BOOLEAN),
                ImmutableMap.of(25.5, false, 27.5, true));
        assertFunction(
                "map_filter(map(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k = 25.5E0 OR v = 'xyz')",
                mapType(DOUBLE, createVarcharType(3)),
                ImmutableMap.of(25.5, "abc", 27.5, "xyz"));
        assertFunction(
                "map_filter(map(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k = 25.5E0 OR cardinality(v) = 3)",
                mapType(DOUBLE, new ArrayType(createVarcharType(1))),
                ImmutableMap.of(25.5, ImmutableList.of("a", "b"), 27.5, ImmutableList.of("a", "b", "c")));

        assertFunction(
                "map_filter(map(ARRAY [true, false], ARRAY [25, 26]), (k, v) -> k AND v = 25)",
                mapType(BOOLEAN, INTEGER),
                ImmutableMap.of(true, 25));
        assertFunction(
                "map_filter(map(ARRAY [false, true], ARRAY [25.5E0, 26.5E0]), (k, v) -> k OR v > 100)",
                mapType(BOOLEAN, DOUBLE),
                ImmutableMap.of(true, 26.5));
        Map<Boolean, Boolean> falseToNullMap = new HashMap<>();
        falseToNullMap.put(false, null);
        assertFunction(
                "map_filter(map(ARRAY [true, false], ARRAY [false, null]), (k, v) -> NOT k OR v)",
                mapType(BOOLEAN, BOOLEAN),
                falseToNullMap);
        assertFunction(
                "map_filter(map(ARRAY [false, true], ARRAY ['abc', 'def']), (k, v) -> NOT k AND v = 'abc')",
                mapType(BOOLEAN, createVarcharType(3)),
                ImmutableMap.of(false, "abc"));
        assertFunction(
                "map_filter(map(ARRAY [true, false], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'b', 'c']]), (k, v) -> k OR cardinality(v) = 3)",
                mapType(BOOLEAN, new ArrayType(createVarcharType(1))),
                ImmutableMap.of(true, ImmutableList.of("a", "b"), false, ImmutableList.of("a", "b", "c")));

        assertFunction(
                "map_filter(map(ARRAY ['s0', 's1', 's2'], ARRAY [25, 26, 27]), (k, v) -> k = 's0' OR v = 27)",
                mapType(createVarcharType(2), INTEGER),
                ImmutableMap.of("s0", 25, "s2", 27));
        assertFunction(
                "map_filter(map(ARRAY ['s0', 's1', 's2'], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k = 's0' OR v = 27.5E0)",
                mapType(createVarcharType(2), DOUBLE),
                ImmutableMap.of("s0", 25.5, "s2", 27.5));
        assertFunction(
                "map_filter(map(ARRAY ['s0', 's1', 's2'], ARRAY [false, null, true]), (k, v) -> k = 's0' OR v)",
                mapType(createVarcharType(2), BOOLEAN),
                ImmutableMap.of("s0", false, "s2", true));
        assertFunction(
                "map_filter(map(ARRAY ['s0', 's1', 's2'], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k = 's0' OR v = 'xyz')",
                mapType(createVarcharType(2), createVarcharType(3)),
                ImmutableMap.of("s0", "abc", "s2", "xyz"));
        assertFunction(
                "map_filter(map(ARRAY ['s0', 's1', 's2'], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k = 's0' OR cardinality(v) = 3)",
                mapType(createVarcharType(2), new ArrayType(createVarcharType(1))),
                ImmutableMap.of("s0", ImmutableList.of("a", "b"), "s2", ImmutableList.of("a", "b", "c")));

        assertFunction(
                "map_filter(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY [25, 26, 27]), (k, v) -> k = ARRAY [1, 2] OR v = 27)",
                mapType(new ArrayType(INTEGER), INTEGER),
                ImmutableMap.of(ImmutableList.of(1, 2), 25, ImmutableList.of(), 27));
        assertFunction(
                "map_filter(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k = ARRAY [1, 2] OR v = 27.5E0)",
                mapType(new ArrayType(INTEGER), DOUBLE),
                ImmutableMap.of(ImmutableList.of(1, 2), 25.5, ImmutableList.of(), 27.5));
        assertFunction(
                "map_filter(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY [false, null, true]), (k, v) -> k = ARRAY [1, 2] OR v)",
                mapType(new ArrayType(INTEGER), BOOLEAN),
                ImmutableMap.of(ImmutableList.of(1, 2), false, ImmutableList.of(), true));
        assertFunction(
                "map_filter(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k = ARRAY [1, 2] OR v = 'xyz')",
                mapType(new ArrayType(INTEGER), createVarcharType(3)),
                ImmutableMap.of(ImmutableList.of(1, 2), "abc", ImmutableList.of(), "xyz"));
        assertFunction(
                "map_filter(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'b', 'c'], ARRAY ['a', 'c']]), (k, v) -> cardinality(k) = 0 OR cardinality(v) = 3)",
                mapType(new ArrayType(INTEGER), new ArrayType(createVarcharType(1))),
                ImmutableMap.of(ImmutableList.of(3, 4), ImmutableList.of("a", "b", "c"), ImmutableList.of(), ImmutableList.of("a", "c")));
    }
}
