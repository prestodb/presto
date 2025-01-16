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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestNoValuesMatchFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction(
                "NO_VALUES_MATCH(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), (x) -> x > 3)",
                BOOLEAN,
                true);
        assertFunction(
                "NO_VALUES_MATCH(MAP(ARRAY[4, 5, 6], ARRAY[-1, -2, -3]), (x) -> x < -2)",
                BOOLEAN,
                false);
        assertFunction(
                "NO_VALUES_MATCH(MAP(ARRAY['x', 'y', 'z'], ARRAY['ab', 'bc', 'cd']), (x) -> x IS NULL)",
                BOOLEAN,
                true);
        assertFunction(
                "NO_VALUES_MATCH(MAP(ARRAY['x', 'y', 'z'], ARRAY[123.0, 99.5, 1000.99]), (x) -> x > 1.0)",
                BOOLEAN,
                false);
    }

    @Test
    public void testEmpty()
    {
        assertFunction("NO_VALUES_MATCH(MAP(ARRAY[], ARRAY[]), (x) -> x > 0)", BOOLEAN, true);
        assertFunction("NO_VALUES_MATCH(MAP(ARRAY[], ARRAY[]), (x) -> x IS NULL)", BOOLEAN, true);
        assertFunction("NO_VALUES_MATCH(MAP(), (x) -> FALSE)", BOOLEAN, true);
    }

    @Test
    public void testNull()
    {
        assertFunction("NO_VALUES_MATCH(NULL, (x) -> x LIKE '%ab%')", BOOLEAN, null);
        assertFunction("NO_VALUES_MATCH(MAP(ARRAY[1, 2], ARRAY['x', 'y']), (x) -> CAST(NULL AS BOOLEAN))", BOOLEAN, null);
        assertFunction("NO_VALUES_MATCH(MAP(ARRAY[1, 2], ARRAY['x', 'y']), (x) -> IF(x = 'x', false, CAST(NULL AS BOOLEAN)))", BOOLEAN, null);
        assertFunction("NO_VALUES_MATCH(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z']), (x) -> IF(x IN ('x', 'y'), false, CAST(NULL AS BOOLEAN)))", BOOLEAN, null);
        assertFunction("NO_VALUES_MATCH(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z']), (x) -> IF(x = 'x', false, IF(x = 'y', true, CAST(NULL AS BOOLEAN))))", BOOLEAN, false);
    }

    @Test
    public void testComplexValues()
    {
        assertFunction(
                "NO_VALUES_MATCH(MAP(ARRAY[1, 2], ARRAY[ROW('x', 1), ROW('y', 2)]), (x) -> x[1] = 'x')",
                BOOLEAN,
                false);
        assertFunction(
                "NO_VALUES_MATCH(MAP(ARRAY[2, 1], ARRAY[ROW('x', 1), ROW('x', -2)]), (x) -> x[2] >= 2)",
                BOOLEAN,
                true);
        assertFunction(
                "NO_VALUES_MATCH(MAP(ARRAY[100, 200, 300], ARRAY[ROW('x', 1), ROW('x', -2), ROW('y', 1)]), (x) -> x[1] = 'ab')",
                BOOLEAN,
                true);
    }

    @Test
    public void testError()
    {
        assertInvalidFunction(
                "NO_VALUES_MATCH(MAP(ARRAY[1, 2], ARRAY[ROW('x', 1), ROW('y', 2)]), (x) -> x[2] LIKE '%ab%')",
                SemanticErrorCode.TYPE_MISMATCH);
        assertInvalidFunction(
                "NO_VALUES_MATCH(MAP(ARRAY[1, 2, 3], ARRAY[4, 5, 6]))",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "NO_VALUES_MATCH(MAP(ARRAY['a', 'b', 'c'], ARRAY[4, 5, 6]), 1)",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
    }
}
