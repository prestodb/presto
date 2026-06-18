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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestArrayTrimFunction
        extends AbstractTestFunctions
{
    @Test
    public void testTrimArray()
    {
        assertFunction("trim_array(ARRAY[1, 2, 3, 4], 0)", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3, 4));
        assertFunction("trim_array(ARRAY[1, 2, 3, 4], 1)", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("trim_array(ARRAY[1, 2, 3, 4], 2)", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("trim_array(ARRAY[1, 2, 3, 4], 3)", new ArrayType(INTEGER), ImmutableList.of(1));
        assertFunction("trim_array(ARRAY[1, 2, 3, 4], 4)", new ArrayType(INTEGER), ImmutableList.of());

        assertFunction("trim_array(ARRAY['a', 'b', 'c', 'd'], 1)", new ArrayType(createVarcharType(1)), ImmutableList.of("a", "b", "c"));
        assertFunction("trim_array(ARRAY['a', 'b', null, 'd'], 1)", new ArrayType(createVarcharType(1)), asList("a", "b", null));
        assertFunction("trim_array(ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5, 6]], 1)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2, 3)));

        assertInvalidFunction("trim_array(ARRAY[1, 2, 3, 4], 5)", "size must not exceed array cardinality 4: 5");
        assertInvalidFunction("trim_array(ARRAY[1, 2, 3, 4], -1)", "size must not be negative: -1");
    }
}
