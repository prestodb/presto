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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class TestMapRemoveNullValuesFunction
        extends AbstractTestFunctions
{
    @Test
    public void test()
    {
        assertFunction(
                "MAP_REMOVE_NULL_VALUES(MAP(ARRAY[1, 2, 3], ARRAY[4, 5, 6]))",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(1, 4, 2, 5, 3, 6));
        assertFunction(
                "MAP_REMOVE_NULL_VALUES(MAP(ARRAY[-1, -2, -3], ARRAY[null, 5.0, null]))",
                mapType(INTEGER, createDecimalType(2, 1)),
                ImmutableMap.of(-2, decimal("5.0")));
        assertFunction(
                "MAP_REMOVE_NULL_VALUES(MAP(ARRAY['ab', 'bc', 'cd'], ARRAY[null, null, null]))",
                mapType(createVarcharType(2), UNKNOWN),
                emptyMap());
        assertFunction(
                "MAP_REMOVE_NULL_VALUES(MAP(ARRAY[123.0, 99.5, 1000.99], ARRAY['x', 'y', 'z']))",
                mapType(createDecimalType(6, 2), createVarcharType(1)),
                ImmutableMap.of(decimal("123.00"), "x", decimal("99.50"), "y", decimal("1000.99"), "z"));
        assertFunction(
                "MAP_REMOVE_NULL_VALUES(MAP(ARRAY['a', 'b', 'c'], ARRAY[ARRAY[1], ARRAY[], ARRAY[null]]))",
                mapType(createVarcharType(1), new ArrayType(INTEGER)),
                ImmutableMap.of("a", singletonList(1), "b", emptyList(), "c", singletonList(null)));
        assertFunction(
                "MAP_REMOVE_NULL_VALUES(MAP(ARRAY[], ARRAY[]))",
                mapType(UNKNOWN, UNKNOWN),
                emptyMap());
        assertFunction(
                "MAP_REMOVE_NULL_VALUES(null)",
                mapType(UNKNOWN, UNKNOWN),
                null);
    }
}
