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

import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestMapTopNKeysFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[1, 2, 3], ARRAY[4, 5, 6]), 2)", new ArrayType(INTEGER), asList(3, 2));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[-1, -2, -3], ARRAY[4, 5, 6]), 2)", new ArrayType(INTEGER), asList(-1, -2));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY['ab', 'bc', 'cd'], ARRAY['x', 'y', 'z']), 1)", new ArrayType(createVarcharType(2)), asList("cd"));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[123.0, 99.5, 1000.99], ARRAY['x', 'y', 'z']), 3)", new ArrayType(createDecimalType(6, 2)), asList(decimal("1000.99"), decimal("123.00"), decimal("99.50")));
    }

    @Test
    public void testNonPositiveN()
    {
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[1, 2, 3], ARRAY[4, 5, 6]), -1)", new ArrayType(INTEGER), asList(1));
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY['a', 'b', 'c'], ARRAY[4, 5, 6]), -2)", new ArrayType(createVarcharType(1)), asList("a", "b"));
    }

    @Test
    public void testEmpty()
    {
        assertFunction("MAP_TOP_N_KEYS(MAP(ARRAY[], ARRAY[]), 5)", new ArrayType(UNKNOWN), asList());
    }

    @Test
    public void testNull()
    {
        assertFunction("MAP_TOP_N_KEYS(NULL, 1)", new ArrayType(UNKNOWN), null);
    }
}
