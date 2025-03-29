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

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestArraySortFunction
        extends AbstractTestFunctions
{
    @Test
    public void testArraySort()
    {
        assertFunction("array_sort(ARRAY [5, 20, null, 5, 3, 50]) ", new ArrayType(INTEGER),
                asList(3, 5, 5, 20, 50, null));
        assertFunction("array_sort(array['x', 'a', 'a', 'a', 'a', 'm', 'j', 'p'])",
                new ArrayType(createVarcharType(1)), ImmutableList.of("a", "a", "a", "a", "j", "m", "p", "x"));
        assertFunction("array_sort(sequence(-4, 3))", new ArrayType(BIGINT),
                asList(-4L, -3L, -2L, -1L, 0L, 1L, 2L, 3L));
        assertFunction("array_sort(reverse(sequence(-4, 3)))", new ArrayType(BIGINT),
                asList(-4L, -3L, -2L, -1L, 0L, 1L, 2L, 3L));
        assertFunction("repeat(1,4)", new ArrayType(INTEGER), asList(1, 1, 1, 1));
        assertFunction("cast(array[] as array<int>)", new ArrayType(INTEGER), asList());
    }
}
