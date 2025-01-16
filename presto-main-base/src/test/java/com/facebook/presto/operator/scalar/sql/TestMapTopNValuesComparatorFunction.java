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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;

public class TestMapTopNValuesComparatorFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 2, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 2, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), ImmutableList.of(3, 2));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY['x', 'y', 'z'], ARRAY['a', 'b', 'c']), 3, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(1)), ImmutableList.of("c", "b", "a"));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY['x', 'y', 'z'], ARRAY['a1', 'b2', 'c3']), 1, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(2)), ImmutableList.of("c3"));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY['a', 'b', 'c', 'd'], ARRAY['xyz', 'xy', 'yz', 'z']), 4, (x, y) -> CASE " +
                        "WHEN LENGTH(x) > LENGTH(y) THEN 1 " +
                        "WHEN LENGTH(x) < LENGTH(y) THEN -1 " +
                        "WHEN x > y THEN 1 " +
                        "WHEN x < y THEN -1 " +
                        "ELSE -1 END)",
                new ArrayType(createVarcharType(3)), ImmutableList.of("xyz", "yz", "xy", "z"));
    }

    @Test
    public void testNLargerThanMapSize()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 8, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 9, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), ImmutableList.of(3, 2, 1));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY['x', 'y', 'z'], ARRAY['abc', 'bcd', 'cde']), 10, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(3)), ImmutableList.of("cde", "bcd", "abc"));
    }

    @Test
    public void testNegativeN()
    {
        assertInvalidFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), -1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", StandardErrorCode.GENERIC_USER_ERROR, "n must be greater than or equal to 0");
        assertInvalidFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1.99, -2.12, 3.01]), -2, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", StandardErrorCode.GENERIC_USER_ERROR, "n must be greater than or equal to 0");
        assertInvalidFunction("MAP_TOP_N_VALUES(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z']), -3, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", StandardErrorCode.GENERIC_USER_ERROR, "n must be greater than or equal to 0");
        assertInvalidFunction("MAP_TOP_N_VALUES(MAP(ARRAY['a', 'b', 'c', 'd'], ARRAY['xyz', 'xy', 'yz', 'z']), -2, (x, y) -> CASE " +
                        "WHEN LENGTH(x) > LENGTH(y) THEN 1 " +
                        "WHEN LENGTH(x) < LENGTH(y) THEN -1 " +
                        "WHEN x > y THEN 1 " +
                        "WHEN x < y THEN -1 " +
                        "ELSE -1 END)",
                StandardErrorCode.GENERIC_USER_ERROR, "n must be greater than or equal to 0");
    }

    @Test
    public void testZeroN()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 0, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 0, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z']), 0, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(1)), ImmutableList.of());
    }

    @Test
    public void testEmpty()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[], ARRAY[]), 1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(UNKNOWN), ImmutableList.of());
    }

    @Test
    public void testNull()
    {
        assertFunction("MAP_TOP_N_VALUES(NULL, 1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(UNKNOWN), null);
    }

    @Test
    public void testComplexValues()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY['a', 'b', 'c'], ARRAY[ROW('x', 1), ROW('y', 2), ROW('z', 3)]), 3," +
                        "(x, y) -> IF(x[1] < y[1], 1, IF(x[1] = y[1], 0, -1)))",
                new ArrayType(RowType.from(ImmutableList.of(RowType.field(createVarcharType(1)), RowType.field(INTEGER)))),
                ImmutableList.of(ImmutableList.of("x", 1), ImmutableList.of("y", 2), ImmutableList.of("z", 3)));
    }

    @Test
    public void testBadLambda()
    {
        assertInvalidFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 1, (x, y) -> 10)", StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Lambda comparator must return either -1, 0, or 1");
        assertInvalidFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 2, null)", SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 3, (x, y) -> IF(x = 'test', 1, -1))", SemanticErrorCode.TYPE_MISMATCH);
    }
}
