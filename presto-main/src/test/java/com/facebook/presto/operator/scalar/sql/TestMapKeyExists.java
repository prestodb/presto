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
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestMapKeyExists
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY[1, 2, 3], ARRAY[4, 5, 6]), 2)",
                BOOLEAN,
                true);
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY[-1, -2, -3], ARRAY[4, 5, 6]), 2)",
                BOOLEAN,
                false);
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY['ab', 'bc', 'cd'], ARRAY['x', 'y', 'z']), 'abc')",
                BOOLEAN,
                false);
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY[123.0, 99.5, 1000.99], ARRAY['x', 'y', 'z']), 123.0)",
                BOOLEAN,
                true);
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY['x', 'y'], ARRAY[NULL, 1]), 'x')",
                BOOLEAN,
                true);
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY['x', 'y'], ARRAY[NULL, NULL]), 'z')",
                BOOLEAN,
                false);
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY[NAN(), 123.21], ARRAY['val1', 'val2']), 123.21)",
                BOOLEAN,
                true);
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY[NAN(), 123.21], ARRAY['val1', 'val2']), NAN())",
                BOOLEAN,
                true);
    }

    @Test
    public void testEmpty()
    {
        assertFunction("MAP_KEY_EXISTS(MAP(ARRAY[], ARRAY[]), 5)", BOOLEAN, false);
    }

    @Test
    public void testNull()
    {
        assertFunction("MAP_KEY_EXISTS(NULL, 1)", BOOLEAN, null);
    }

    @Test
    public void testComplexKeys()
    {
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY[ROW('x', 1), ROW('y', 2)], ARRAY[1, 2]), ROW('x', 1))",
                BOOLEAN,
                true);
        assertFunction(
                "MAP_KEY_EXISTS(MAP(ARRAY[ROW('x', 1), ROW('x', -2)], ARRAY[2, 1]), ROW('y', 1))",
                BOOLEAN,
                false);
    }
}
