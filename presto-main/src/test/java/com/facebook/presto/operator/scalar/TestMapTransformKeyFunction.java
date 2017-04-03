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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.UnknownType.UNKNOWN;

public class TestMapTransformKeyFunction
        extends AbstractTestFunctions
{
    @Test
    public void testEmpty()
            throws Exception
    {
        assertFunction("transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> NULL)", new MapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> k)", new MapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> v)", new MapType(UNKNOWN, UNKNOWN), ImmutableMap.of());

        assertFunction("transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 0)", new MapType(INTEGER, UNKNOWN), ImmutableMap.of());
        assertFunction("transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> true)", new MapType(BOOLEAN, UNKNOWN), ImmutableMap.of());
        assertFunction("transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 'key')", new MapType(createVarcharType(3), UNKNOWN), ImmutableMap.of());
        assertFunction("transform_keys(CAST (map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> k + CAST(v as BIGINT))", new MapType(BIGINT, VARCHAR), ImmutableMap.of());
        assertFunction("transform_keys(CAST (map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> v)", new MapType(VARCHAR, VARCHAR), ImmutableMap.of());
    }

    @Test
    public void testNullKey()
            throws Exception
    {
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2, 3], ARRAY ['a', 'b', 'c']), (k, v) -> NULL)", "map key cannot be null");
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2, 3], ARRAY ['a', 'b', NULL]), (k, v) -> v)", "map key cannot be null");
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2, 3], ARRAY [1, 2, NULL]), (k, v) -> k + v)", "map key cannot be null");
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2, 3], ARRAY ['1', '2', 'Invalid']), (k, v) -> TRY_CAST(v as BIGINT))", "map key cannot be null");
        assertInvalidFunction(
                "transform_keys(map(ARRAY[1, 2, 3], ARRAY [1.0, 1.4, 1.7]), (k, v) -> element_at(map(ARRAY[1, 2], ARRAY['one', 'two']), k))",
                "map key cannot be null");
    }

    @Test
    public void testDuplicateKeys()
            throws Exception
    {
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2, 3, 4], ARRAY ['a', 'b', 'c', 'd']), (k, v) -> k % 3)", "Duplicate keys (1) are not allowed");
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2, 3], ARRAY ['a', 'b', 'c']), (k, v) -> k % 2 = 0)", "Duplicate keys (false) are not allowed");
        assertInvalidFunction("transform_keys(map(ARRAY[1.5, 2.5, 3.5], ARRAY ['a', 'b', 'c']), (k, v) -> k - floor(k))", "Duplicate keys (0.5) are not allowed");
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2, 3, 4], ARRAY ['a', 'b', 'c', 'b']), (k, v) -> v)", "Duplicate keys (b) are not allowed");
        assertInvalidFunction("transform_keys(map(ARRAY['abc1', 'cba2', 'abc3'], ARRAY [1, 2, 3]), (k, v) -> substr(k, 1, 3))", "Duplicate keys (abc) are not allowed");

        assertInvalidFunction("transform_keys(map(ARRAY[ARRAY [1], ARRAY [2]], ARRAY [2, 1]), (k, v) -> array_sort(k || v))", "Duplicate keys ([1, 2]) are not allowed");
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2], ARRAY [null, null]), (k, v) -> DATE '2001-08-22')", "Duplicate keys (2001-08-22) are not allowed");
        assertInvalidFunction("transform_keys(map(ARRAY[1, 2], ARRAY [null, null]), (k, v) -> TIMESTAMP '2001-08-22 03:04:05.321')", "Duplicate keys (2001-08-22 03:04:05.321) are not allowed");
    }

    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction(
                "transform_keys(map(ARRAY [1, 2, 3, 4], ARRAY [10, 20, 30, 40]), (k, v) -> k + v)",
                new MapType(INTEGER, INTEGER),
                ImmutableMap.of(11, 10, 22, 20, 33, 30, 44, 40));

        assertFunction(
                "transform_keys(map(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, 2, 3, 4]), (k, v) -> v * v)",
                new MapType(INTEGER, INTEGER),
                ImmutableMap.of(1, 1, 4, 2, 9, 3, 16, 4));

        assertFunction(
                "transform_keys(map(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, 2, 3, 4]), (k, v) -> k || CAST(v as VARCHAR))",
                new MapType(VARCHAR, INTEGER),
                ImmutableMap.of("a1", 1, "b2", 2, "c3", 3, "d4", 4));

        assertFunction(
                "transform_keys(map(ARRAY[1, 2, 3], ARRAY [1.0, 1.4, 1.7]), (k, v) -> map(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three'])[k])",
                new MapType(createVarcharType(5), DOUBLE),
                ImmutableMap.of("one", 1.0, "two", 1.4, "three", 1.7));

        Map<String, Integer> expectedStringIntMap = new HashMap<>();
        expectedStringIntMap.put("a1", 1);
        expectedStringIntMap.put("b0", null);
        expectedStringIntMap.put("c3", 3);
        expectedStringIntMap.put("d4", 4);
        assertFunction(
                "transform_keys(map(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, NULL, 3, 4]), (k, v) -> k || COALESCE(CAST(v as VARCHAR), '0'))",
                new MapType(VARCHAR, INTEGER),
                expectedStringIntMap);
    }

    @Test
    public void testTypeCombinations()
            throws Exception
    {
        assertFunction(
                "transform_keys(map(ARRAY [25, 26, 27], ARRAY [25, 26, 27]), (k, v) -> k + v)",
                new MapType(INTEGER, INTEGER),
                ImmutableMap.of(50, 25, 52, 26, 54, 27));
        assertFunction(
                "transform_keys(map(ARRAY [25, 26, 27], ARRAY [25.5, 26.5, 27.5]), (k, v) -> k + v)",
                new MapType(DOUBLE, DOUBLE),
                ImmutableMap.of(50.5, 25.5, 52.5, 26.5, 54.5, 27.5));
        assertFunction(
                "transform_keys(map(ARRAY [25, 26], ARRAY [false, true]), (k, v) -> k % 2 = 0 OR v)",
                new MapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(false, false, true, true));
        assertFunction(
                "transform_keys(map(ARRAY [25, 26, 27], ARRAY ['abc', 'def', 'xyz']), (k, v) -> to_base(k, 16) || substr(v, 1, 1))",
                new MapType(VARCHAR, createVarcharType(3)),
                ImmutableMap.of("19a", "abc", "1ad", "def", "1bx", "xyz"));
        assertFunction(
                "transform_keys(map(ARRAY [25, 26], ARRAY [ARRAY ['a'], ARRAY ['b']]), (k, v) -> ARRAY [CAST(k AS VARCHAR)] || v)",
                new MapType(new ArrayType(VARCHAR), new ArrayType(createVarcharType(1))),
                ImmutableMap.of(ImmutableList.of("25", "a"), ImmutableList.of("a"), ImmutableList.of("26", "b"), ImmutableList.of("b")));

        assertFunction(
                "transform_keys(map(ARRAY [25.5, 26.5, 27.5], ARRAY [25, 26, 27]), (k, v) -> CAST(k * 2 AS BIGINT) + v)",
                new MapType(BIGINT, INTEGER),
                ImmutableMap.of(76L, 25, 79L, 26, 82L, 27));
        assertFunction(
                "transform_keys(map(ARRAY [25.5, 26.5, 27.5], ARRAY [25.5, 26.5, 27.5]), (k, v) -> k + v)",
                new MapType(DOUBLE, DOUBLE),
                ImmutableMap.of(51.0, 25.5, 53.0, 26.5, 55.0, 27.5));
        assertFunction(
                "transform_keys(map(ARRAY [25.2, 26.2], ARRAY [false, true]), (k, v) -> CAST(k AS BIGINT) % 2 = 0 OR v)",
                new MapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(false, false, true, true));
        assertFunction(
                "transform_keys(map(ARRAY [25.5, 26.5, 27.5], ARRAY ['abc', 'def', 'xyz']), (k, v) -> CAST(k AS VARCHAR) || substr(v, 1, 1))",
                new MapType(VARCHAR, createVarcharType(3)),
                ImmutableMap.of("25.5a", "abc", "26.5d", "def", "27.5x", "xyz"));
        assertFunction(
                "transform_keys(map(ARRAY [25.5, 26.5], ARRAY [ARRAY ['a'], ARRAY ['b']]), (k, v) -> ARRAY [CAST(k AS VARCHAR)] || v)",
                new MapType(new ArrayType(VARCHAR), new ArrayType(createVarcharType(1))),
                ImmutableMap.of(ImmutableList.of("25.5", "a"), ImmutableList.of("a"), ImmutableList.of("26.5", "b"), ImmutableList.of("b")));

        assertFunction(
                "transform_keys(map(ARRAY [true, false], ARRAY [25, 26]), (k, v) -> if(k, 2 * v, 3 * v))",
                new MapType(INTEGER, INTEGER),
                ImmutableMap.of(50, 25, 78, 26));
        assertFunction(
                "transform_keys(map(ARRAY [false, true], ARRAY [25.5, 26.5]), (k, v) -> if(k, 2 * v, 3 * v))",
                new MapType(DOUBLE, DOUBLE),
                ImmutableMap.of(76.5, 25.5, 53.0, 26.5));
        Map<Boolean, Boolean> expectedBoolBoolMap = new HashMap<>();
        expectedBoolBoolMap.put(false, true);
        expectedBoolBoolMap.put(true, null);
        assertFunction(
                "transform_keys(map(ARRAY [true, false], ARRAY [true, NULL]), (k, v) -> if(k, NOT v, v IS NULL))",
                new MapType(BOOLEAN, BOOLEAN),
                expectedBoolBoolMap);
        assertFunction(
                "transform_keys(map(ARRAY [false, true], ARRAY ['abc', 'def']), (k, v) -> if(k, substr(v, 1, 2), substr(v, 1, 1)))",
                new MapType(createVarcharType(3), createVarcharType(3)),
                ImmutableMap.of("a", "abc", "de", "def"));
        assertFunction(
                "transform_keys(map(ARRAY [true, false], ARRAY [ARRAY ['a', 'b'], ARRAY ['x', 'y']]), (k, v) -> if(k, reverse(v), v))",
                new MapType(new ArrayType(createVarcharType(1)), new ArrayType(createVarcharType(1))),
                ImmutableMap.of(ImmutableList.of("b", "a"), ImmutableList.of("a", "b"), ImmutableList.of("x", "y"), ImmutableList.of("x", "y")));

        assertFunction(
                "transform_keys(map(ARRAY ['a', 'ab', 'abc'], ARRAY [25, 26, 27]), (k, v) -> length(k) + v)",
                new MapType(BIGINT, INTEGER),
                ImmutableMap.of(26L, 25, 28L, 26, 30L, 27));
        assertFunction(
                "transform_keys(map(ARRAY ['a', 'ab', 'abc'], ARRAY [25.5, 26.5, 27.5]), (k, v) -> length(k) + v)",
                new MapType(DOUBLE, DOUBLE),
                ImmutableMap.of(26.5, 25.5, 28.5, 26.5, 30.5, 27.5));
        assertFunction(
                "transform_keys(map(ARRAY ['a', 'b'], ARRAY [false, true]), (k, v) -> k = 'b' OR v)",
                new MapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(false, false, true, true));
        assertFunction(
                "transform_keys(map(ARRAY ['a', 'x'], ARRAY ['bc', 'yz']), (k, v) -> k || v)",
                new MapType(VARCHAR, createVarcharType(2)),
                ImmutableMap.of("abc", "bc", "xyz", "yz"));
        assertFunction(
                "transform_keys(map(ARRAY ['x', 'y'], ARRAY [ARRAY ['a'], ARRAY ['b']]), (k, v) -> k || v)",
                new MapType(new ArrayType(createVarcharType(1)), new ArrayType(createVarcharType(1))),
                ImmutableMap.of(ImmutableList.of("x", "a"), ImmutableList.of("a"), ImmutableList.of("y", "b"), ImmutableList.of("b")));

        assertFunction(
                "transform_keys(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [25, 26]), (k, v) -> reduce(k, 0, (s, x) -> s + x, s -> s) + v)",
                new MapType(INTEGER, INTEGER),
                ImmutableMap.of(28, 25, 33, 26));
        assertFunction(
                "transform_keys(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [25.5, 26.5]), (k, v) -> reduce(k, 0, (s, x) -> s + x, s -> s) + v)",
                new MapType(DOUBLE, DOUBLE),
                ImmutableMap.of(28.5, 25.5, 33.5, 26.5));
        assertFunction(
                "transform_keys(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [false, true]), (k, v) -> contains(k, 3) AND v)",
                new MapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(false, false, true, true));
        assertFunction(
                "transform_keys(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY ['abc', 'xyz']), (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v)",
                new MapType(new ArrayType(VARCHAR), createVarcharType(3)),
                ImmutableMap.of(ImmutableList.of("1", "2", "abc"), "abc", ImmutableList.of("3", "4", "xyz"), "xyz"));
        assertFunction(
                "transform_keys(map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [ARRAY ['a'], ARRAY ['a', 'b']]), (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v)",
                new MapType(new ArrayType(VARCHAR), new ArrayType(createVarcharType(1))),
                ImmutableMap.of(ImmutableList.of("1", "2", "a"), ImmutableList.of("a"), ImmutableList.of("3", "4", "a", "b"), ImmutableList.of("a", "b")));
    }
}
