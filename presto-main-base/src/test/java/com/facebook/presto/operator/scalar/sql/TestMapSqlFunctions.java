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
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;

public class TestMapSqlFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testMapIntKeysToArray()
    {
        assertFunction("MAP_INT_KEYS_TO_ARRAY(CAST(MAP() AS MAP<INT, INT>))",
                new ArrayType(INTEGER),
                null);

        assertFunction("MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[1, 3], ARRAY['a', 'b']))",
                new ArrayType(createVarcharType(1)),
                asList("a", null, "b"));

        assertFunction("MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[3, 5, 6, 9], ARRAY['a', 'b', 'c', 'd']))",
                new ArrayType(createVarcharType(1)),
                asList(null, null, "a", null, "b", "c", null, null, "d"));

        assertFunction("MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[3, 5, 6, 9], ARRAY['a', null, 'c', 'd']))",
                new ArrayType(createVarcharType(1)),
                asList(null, null, "a", null, null, "c", null, null, "d"));

        assertFunction("MAP_INT_KEYS_TO_ARRAY(CAST(MAP() AS MAP<INT, INT>))",
                new ArrayType(INTEGER),
                null);

        assertFunction("MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[2], ARRAY[MAP(ARRAY[3, 5, 6, 9], ARRAY['a', null, 'c', 'd'])]))",
                new ArrayType(mapType(INTEGER, createVarcharType(1))),
                asList(null, asMap(asList(3, 5, 6, 9), asList("a", null, "c", "d"))));

        assertFunction("MAP_INT_KEYS_TO_ARRAY(CAST(MAP() AS MAP<INT, INT>))",
                new ArrayType(INTEGER),
                null);

        assertInvalidFunction(
                "MAP_INT_KEYS_TO_ARRAY(MAP(CAST(SEQUENCE(1,10000)||ARRAY[10001] AS ARRAY<INT>),SEQUENCE(1,10000)||ARRAY[10001]))",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Max key value must be <= 10k for map_int_keys_to_array function");

        assertInvalidFunction(
                "MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[0],ARRAY[1]))",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Only positive keys allowed in map_int_keys_to_array function, but got: 0");

        assertInvalidFunction(
                "MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[-1, 2], ARRAY['a', 'b']))",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Only positive keys allowed in map_int_keys_to_array function, but got: -1");

        assertInvalidFunction("MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[0, 2], ARRAY['a', 'b']))",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Only positive keys allowed in map_int_keys_to_array function, but got: 0");
    }

    @Test
    public void testArrayToMapIntKeys()
    {
        assertFunction("ARRAY_TO_MAP_INT_KEYS(CAST(ARRAY[3, 5, 6, 9] AS ARRAY<INT>))",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(1, 3, 2, 5, 3, 6, 4, 9));

        assertFunction("ARRAY_TO_MAP_INT_KEYS(CAST(ARRAY[3, 5, null, 6, 9] AS ARRAY<INT>))",
                mapType(INTEGER, INTEGER),
                asMap(asList(1, 2, 4, 5), asList(3, 5, 6, 9)));

        assertFunction("ARRAY_TO_MAP_INT_KEYS(CAST(ARRAY[3, 5, null, 6, 9, null, null, 1] AS ARRAY<INT>))",
                mapType(INTEGER, INTEGER),
                asMap(asList(1, 2, 4, 5, 8), asList(3, 5, 6, 9, 1)));

        assertFunction("ARRAY_TO_MAP_INT_KEYS(CAST(NULL AS ARRAY<INT>))",
                mapType(INTEGER, INTEGER),
                null);

        assertInvalidFunction(
                "ARRAY_TO_MAP_INT_KEYS(SEQUENCE(1,10000)||ARRAY[10001])",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Max number of elements must be <= 10k for array_to_map_int_keys function");
    }
}
