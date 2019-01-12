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
import io.prestosql.spi.type.RowType;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;

public class TestMapZipWithFunction
        extends AbstractTestFunctions
{
    @Test
    public void testRetainedSizeBounded()
    {
        assertCachedInstanceHasBoundedRetainedSize("map_zip_with(" +
                "map(ARRAY [25, 26, 27], ARRAY [25, 26, 27]), " +
                "map(ARRAY [24, 25, 26], ARRAY [24, 25, 26]), " +
                "(k, v1, v2) -> v1 + v2)");
    }

    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY [1, 2, 3], ARRAY [10, 20, 30]), " +
                        "map(ARRAY [1, 2, 3], ARRAY [1, 4, 9]), " +
                        "(k, v1, v2) -> k + v1 + v2)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(1, 12, 2, 26, 3, 42));

        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY ['a', 'b'], ARRAY [1, 2]), " +
                        "map(ARRAY ['c', 'd'], ARRAY [30, 40]), " +
                        "(k, v1, v2) -> v1)",
                mapType(createVarcharType(1), INTEGER),
                asMap(asList("a", "b", "c", "d"), asList(1, 2, null, null)));

        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY ['a', 'b'], ARRAY [1, 2]), " +
                        "map(ARRAY ['c', 'd'], ARRAY [30, 40]), " +
                        "(k, v1, v2) -> v2)",
                mapType(createVarcharType(1), INTEGER),
                asMap(asList("a", "b", "c", "d"), asList(null, null, 30, 40)));

        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY ['a', 'b', 'c'], ARRAY [1, 2, 3]), " +
                        "map(ARRAY ['b', 'c', 'd', 'e'], ARRAY ['x', 'y', 'z', null]), " +
                        "(k, v1, v2) -> (v1, v2))",
                mapType(createVarcharType(1), RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(1)))),
                ImmutableMap.of(
                        "a", asList(1, null),
                        "b", ImmutableList.of(2, "x"),
                        "c", ImmutableList.of(3, "y"),
                        "d", asList(null, "z"),
                        "e", asList(null, null)));
    }

    @Test
    public void testTypes()
            throws Exception
    {
        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY [25, 26, 27], ARRAY [25, 26, 27]), " +
                        "map(ARRAY [25, 26, 27], ARRAY [1, 2, 3]), " +
                        "(k, v1, v2) -> v1 * v2 - k)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(25, 0, 26, 26, 27, 54));
        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY [25.5E0, 26.75E0, 27.875E0], ARRAY [25, 26, 27]), " +
                        "map(ARRAY [25.5E0, 26.75E0, 27.875E0], ARRAY [1, 2, 3]), " +
                        "(k, v1, v2) -> v1 + v2 - k)",
                mapType(DOUBLE, DOUBLE),
                ImmutableMap.of(25.5, 0.5, 26.75, 1.25, 27.875, 2.125));
        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY [true, false], ARRAY [25, 26]), " +
                        "map(ARRAY [true, false], ARRAY [1, 2]), " +
                        "(k, v1, v2) -> k AND v1 % v2 = 0)",
                mapType(BOOLEAN, BOOLEAN),
                ImmutableMap.of(true, true, false, false));
        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY ['s0', 's1', 's2'], ARRAY [25, 26, 27]), " +
                        "map(ARRAY ['s0', 's1', 's2'], ARRAY [1, 2, 3]), " +
                        "(k, v1, v2) -> k || ':' || CAST(v1/v2 AS VARCHAR))",
                mapType(createVarcharType(2), VARCHAR),
                ImmutableMap.of("s0", "s0:25", "s1", "s1:13", "s2", "s2:9"));
        assertFunction(
                "map_zip_with(" +
                        "map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [25, 26]), " +
                        "map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [5, 6]), " +
                        "(k, v1, v2) -> if(v1 % v2 = 0, reverse(k), k))",
                mapType(new ArrayType(INTEGER), new ArrayType(INTEGER)),
                ImmutableMap.of(ImmutableList.of(1, 2), ImmutableList.of(2, 1), ImmutableList.of(3, 4), ImmutableList.of(3, 4)));
    }
}
