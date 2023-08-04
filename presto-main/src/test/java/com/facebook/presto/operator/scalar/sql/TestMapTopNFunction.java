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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;

public class TestMapTopNFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY[1, 2, 3], ARRAY[4, 5, 6]), 2)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(3, 6, 2, 5));
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY[-1, -2, -3], ARRAY[4, 5, 6]), 2)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(-3, 6, -2, 5));
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY['ab', 'bc', 'cd'], ARRAY['x', 'y', 'z']), 1)",
                mapType(createVarcharType(2), createVarcharType(1)),
                ImmutableMap.of("cd", "z"));
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY[123.0, 99.5, 1000.99], ARRAY['x', 'y', 'z']), 3)",
                mapType(createDecimalType(6, 2), createVarcharType(1)),
                ImmutableMap.of(decimal("1000.99"), "z", decimal("99.50"), "y", decimal("123.00"), "x"));
    }

    @Test
    public void testNegativeN()
    {
        assertInvalidFunction(
                "MAP_TOP_N(MAP(ARRAY[100, 200, 300], ARRAY[4, 5, 6]), -3)",
                StandardErrorCode.GENERIC_USER_ERROR,
                "n must be greater than or equal to 0");
        assertInvalidFunction(
                "MAP_TOP_N(MAP(ARRAY[1, 2, 3], ARRAY[4, 5, 6]), -1)",
                StandardErrorCode.GENERIC_USER_ERROR,
                "n must be greater than or equal to 0");
        assertInvalidFunction(
                "MAP_TOP_N(MAP(ARRAY['a', 'b', 'c'], ARRAY[4, 5, 6]), -2)",
                StandardErrorCode.GENERIC_USER_ERROR,
                "n must be greater than or equal to 0");
    }

    @Test
    public void testZeroN()
    {
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY[-1, -2, -3], ARRAY[4, 5, 6]), 0)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of());
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY['ab', 'bc', 'cd'], ARRAY['x', 'y', 'z']), 0)",
                mapType(createVarcharType(2), createVarcharType(1)),
                ImmutableMap.of());
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY[123.0, 99.5, 1000.99], ARRAY['x', 'y', 'z']), 0)",
                mapType(createDecimalType(6, 2), createVarcharType(1)),
                ImmutableMap.of());
    }

    @Test
    public void testEmpty()
    {
        assertFunction("MAP_TOP_N(MAP(ARRAY[], ARRAY[]), 5)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
    }

    @Test
    public void testNull()
    {
        assertFunction("MAP_TOP_N(NULL, 1)", mapType(UNKNOWN, UNKNOWN), null);
    }

    @Test
    public void testComplexKeys()
    {
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY[ROW('x', 1), ROW('y', 2)], ARRAY[1, 2]), 1)",
                mapType(RowType.from(ImmutableList.of(RowType.field(createVarcharType(1)), RowType.field(INTEGER))), INTEGER),
                ImmutableMap.of(ImmutableList.of("y", 2), 2));
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY[ROW('x', 1), ROW('x', -2)], ARRAY[2, 1]), 1)",
                mapType(RowType.from(ImmutableList.of(RowType.field(createVarcharType(1)), RowType.field(INTEGER))), INTEGER),
                ImmutableMap.of(ImmutableList.of("x", 1), 2));
        assertFunction(
                "MAP_TOP_N(MAP(ARRAY[ROW('x', 1), ROW('x', -2), ROW('y', 1)], ARRAY[100, 200, null]), 3)",
                mapType(RowType.from(ImmutableList.of(RowType.field(createVarcharType(1)), RowType.field(INTEGER))), INTEGER),
                asMap(asList(ImmutableList.of("x", -2), ImmutableList.of("x", 1), ImmutableList.of("y", 1)), asList(200, 100, null)));
    }
}
