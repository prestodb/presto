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
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;

public class TestArrayMinusFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction(
                "ARRAY_MINUS(ARRAY[1, 2, 3], ARRAY[4, 5, 6])",
                new ArrayType(INTEGER),
                ImmutableList.of(1, 2, 3));
        assertFunction(
                "ARRAY_MINUS(ARRAY[-1, -2, -3], ARRAY[-1, -2])",
                new ArrayType(INTEGER),
                ImmutableList.of(-3));
        assertFunction(
                "ARRAY_MINUS(ARRAY['ab', 'bc', 'cd'], ARRAY['x', 'y', 'cd'])",
                new ArrayType(createVarcharType(2)),
                ImmutableList.of("ab", "bc"));
        assertFunction(
                "ARRAY_MINUS(ARRAY[double'123.0', double'99.5', double'1000.99'], ARRAY[double'100.0'])",
                new ArrayType(DOUBLE),
                ImmutableList.of(123.0d, 99.5d, 1000.99d));
    }

    @Test
    public void testEmpty()
    {
        assertFunction("ARRAY_MINUS(ARRAY[], ARRAY[1, 2, 3])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("ARRAY_MINUS(ARRAY[], ARRAY['a', 'b', 'c'])", new ArrayType(createVarcharType(1)), ImmutableList.of());
        assertFunction("ARRAY_MINUS(ARRAY[], ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
    }

    @Test
    public void testNull()
    {
        assertFunction("ARRAY_MINUS(NULL, ARRAY[1, 2, 4])", new ArrayType(INTEGER), null);
        assertFunction("ARRAY_MINUS(ARRAY[1, 2, 4], NULL)", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("ARRAY_MINUS(NULL, NULL)", new ArrayType(UNKNOWN), null);
    }

    @Test
    public void testComplexKeys()
    {
        RowType rowType = RowType.from(ImmutableList.of(RowType.field(createVarcharType(1)), RowType.field(INTEGER)));
        assertFunction(
                "ARRAY_MINUS(ARRAY[ROW('x', 1), ROW('y', 2)], ARRAY[ROW('x', 1), ROW('y', 2)])",
                new ArrayType(rowType),
                ImmutableList.of());
        rowType = RowType.from(ImmutableList.of(RowType.field(INTEGER), RowType.field(createVarcharType(3))));
        assertFunction(
                "ARRAY_MINUS(ARRAY[ROW(1, 'abc'), ROW(-2, 'xyz')], ARRAY[])",
                new ArrayType(rowType),
                ImmutableList.of(ImmutableList.of(1, "abc"), ImmutableList.of(-2, "xyz")));
        rowType = RowType.from(ImmutableList.of(RowType.field(createVarcharType(1)), RowType.field(INTEGER)));
        assertFunction(
                "ARRAY_MINUS(ARRAY[ROW('x', 1), ROW('x', -2), ROW('y', 1)], ARRAY[ROW('y', 2), ROW('x', -2)])",
                new ArrayType(rowType),
                ImmutableList.of(ImmutableList.of("x", 1), ImmutableList.of("y", 1)));
    }

    @Test
    public void testError()
    {
        assertInvalidFunction(
                "ARRAY_MINUS()",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "ARRAY_MINUS(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z'])",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "ARRAY_MINUS(1, 2)",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
    }
}
