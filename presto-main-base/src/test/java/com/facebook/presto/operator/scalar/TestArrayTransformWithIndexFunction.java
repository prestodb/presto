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
import com.facebook.presto.common.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;

public class TestArrayTransformWithIndexFunction
        extends AbstractTestFunctions
{
    public TestArrayTransformWithIndexFunction()
    {
        super(testSessionBuilder().setTimeZoneKey(getTimeZoneKey("Pacific/Kiritimati")).build());
    }

    @Test
    public void testBasic()
    {
        assertFunction("transform_with_index(ARRAY [5, 6], (x, i) -> x + i)", new ArrayType(INTEGER), ImmutableList.of(5, 7));
        assertFunction("transform_with_index(ARRAY [5, 6, 7], (x, i) -> x * i)", new ArrayType(INTEGER), ImmutableList.of(0, 6, 14));
        assertFunction("transform_with_index(ARRAY [10, 20, 30], (x, i) -> i)", new ArrayType(INTEGER), ImmutableList.of(0, 1, 2));
        assertFunction("transform_with_index(ARRAY [10, 20, 30], (x, i) -> x)", new ArrayType(INTEGER), ImmutableList.of(10, 20, 30));
    }

    @Test
    public void testNull()
    {
        assertFunction("transform_with_index(ARRAY [3], (x, i) -> x + i)", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("transform_with_index(ARRAY [NULL, NULL], (x, i) -> x + i)", new ArrayType(INTEGER), asList(null, null));
        assertFunction("transform_with_index(ARRAY [NULL, 3, NULL], (x, i) -> x + i)", new ArrayType(INTEGER), asList(null, 4, null));

        assertFunction("transform_with_index(ARRAY [3], (x, i) -> x IS NULL)", new ArrayType(BOOLEAN), ImmutableList.of(false));
        assertFunction("transform_with_index(ARRAY [NULL, NULL], (x, i) -> x IS NULL)", new ArrayType(BOOLEAN), ImmutableList.of(true, true));
        assertFunction("transform_with_index(ARRAY [NULL, 3, NULL], (x, i) -> x IS NULL)", new ArrayType(BOOLEAN), ImmutableList.of(true, false, true));
        
        // Test index behavior with null values
        assertFunction("transform_with_index(ARRAY [NULL, 3, NULL], (x, i) -> i)", new ArrayType(INTEGER), ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testIndexOnly()
    {
        assertFunction("transform_with_index(ARRAY ['a', 'b', 'c'], (x, i) -> i)", new ArrayType(INTEGER), ImmutableList.of(0, 1, 2));
        assertFunction("transform_with_index(ARRAY [1, 2, 3, 4, 5], (x, i) -> i * 10)", new ArrayType(INTEGER), ImmutableList.of(0, 10, 20, 30, 40));
    }

    @Test
    public void testTypeCombinations()
    {
        // Integer array with index operations
        assertFunction("transform_with_index(ARRAY [25, 26], (x, i) -> x + i)", new ArrayType(INTEGER), ImmutableList.of(25, 27));
        assertFunction("transform_with_index(ARRAY [25, 26], (x, i) -> x + i + 1.0E0)", new ArrayType(DOUBLE), ImmutableList.of(26.0, 28.0));
        assertFunction("transform_with_index(ARRAY [25, 26], (x, i) -> i = 0)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        
        // Double array with index operations
        assertFunction("transform_with_index(ARRAY [25.6E0, 27.3E0], (x, i) -> CAST(x + i AS BIGINT))", new ArrayType(BIGINT), ImmutableList.of(26L, 28L));
        assertFunction("transform_with_index(ARRAY [25.6E0, 27.3E0], (x, i) -> x + i)", new ArrayType(DOUBLE), ImmutableList.of(25.6, 28.3));
        
        // Boolean array with index operations
        assertFunction("transform_with_index(ARRAY [true, false], (x, i) -> if(x, i, i + 10))", new ArrayType(INTEGER), ImmutableList.of(0, 11));
        
        // String array with index operations
        assertFunction("transform_with_index(ARRAY ['abc', 'def'], (x, i) -> x || CAST(i AS VARCHAR))", new ArrayType(createUnboundedVarcharType()), ImmutableList.of("abc0", "def1"));
        
        // Array of arrays with index operations
        assertFunction(
                "transform_with_index(ARRAY [ARRAY ['abc', 'def'], ARRAY ['ghi', 'jkl']], (x, i) -> x[i + 1])",
                new ArrayType(createVarcharType(3)),
                ImmutableList.of("abc", "jkl"));
    }

    @Test
    public void testComplexExpressions()
    {
        // Complex lambda expressions using both element and index
        assertFunction(
                "transform_with_index(ARRAY [10, 20, 30], (x, i) -> ROW(x, i, x + i))",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, INTEGER, INTEGER))),
                ImmutableList.of(
                        ImmutableList.of(10, 0, 10),
                        ImmutableList.of(20, 1, 21),
                        ImmutableList.of(30, 2, 32)
                ));
        
        // Using index for conditional logic
        assertFunction(
                "transform_with_index(ARRAY [1, 2, 3, 4], (x, i) -> if(i % 2 = 0, x * 2, x))",
                new ArrayType(INTEGER),
                ImmutableList.of(2, 2, 6, 4));
    }

    @Test
    public void testEmptyArray()
    {
        assertFunction("transform_with_index(ARRAY [], (x, i) -> x + i)", new ArrayType(INTEGER), ImmutableList.of());
    }

    @Test
    public void testSingleElement()
    {
        assertFunction("transform_with_index(ARRAY [42], (x, i) -> x + i)", new ArrayType(INTEGER), ImmutableList.of(42));
        assertFunction("transform_with_index(ARRAY [42], (x, i) -> i)", new ArrayType(INTEGER), ImmutableList.of(0));
    }
}