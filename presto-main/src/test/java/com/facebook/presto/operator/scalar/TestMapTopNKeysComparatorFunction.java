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
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestMapTopNKeysComparatorFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 2, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(4, 5));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 2, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(6, 5));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY['x', 'y', 'z'], ARRAY[1, 2, 3]), 3, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(1)), asList("z", "y", "x"));
    }

    @Test
    public void testNLargerThanMapSize()
    {
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 8, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(4, 5, 6));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 9, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(6, 5, 4));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY['x', 'y', 'z'], ARRAY[1, 2, 3]), 10, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(1)), asList("z", "y", "x"));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY['abc', 'ab', 'a', 'b'], ARRAY[1, 2, 3, 4]), 10, (x, y) -> CASE " +
                "WHEN LENGTH(x) > LENGTH(y) THEN 1 " +
                "WHEN LENGTH(x) < LENGTH(y) THEN -1 " +
                "WHEN x > y THEN 1 " +
                "WHEN x < y THEN -1 " +
                "ELSE -1 END)",
                new ArrayType(createVarcharType(3)), asList("abc", "ab", "b", "a"));
    }

    @Test
    public void testNegativeN()
    {
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), -1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(6));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), -2, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(4, 5));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY['x', 'y', 'z'], ARRAY[1, 2, 3]), -3, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(1)), asList("x", "y", "z"));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY['abc', 'ab', 'a', 'b'], ARRAY[1, 2, 3, 4]), -2, (x, y) -> CASE " +
                        "WHEN LENGTH(x) > LENGTH(y) THEN 1 " +
                        "WHEN LENGTH(x) < LENGTH(y) THEN -1 " +
                        "WHEN x > y THEN 1 " +
                        "WHEN x < y THEN -1 " +
                        "ELSE -1 END)",
                new ArrayType(createVarcharType(3)), asList("a", "b"));
    }

    @Test
    public void testZeroN()
    {
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 0, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList());
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 0, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList());
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY['x', 'y', 'z'], ARRAY[1, 2, 3]), 0, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(1)), asList());
    }

    @Test
    public void testEmpty()
    {
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[], ARRAY[]), 1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(UNKNOWN), asList());
    }

    @Test
    public void testNull()
    {
        assertFunction("MAP_TOP_N_KEYS(NULL, 1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(UNKNOWN), null);
    }
}
