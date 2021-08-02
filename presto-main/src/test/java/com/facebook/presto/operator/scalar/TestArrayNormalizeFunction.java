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

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class TestArrayNormalizeFunction

        extends AbstractTestFunctions
{
    @Test
    public void test0Norm()
    {
        assertFunction("array_normalize(ARRAY[1.0E0, 2.0E0, 3.3E0], 0.0E0)", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.3));
        assertFunction("array_normalize(ARRAY[REAL '1.0', REAL '2.0', REAL '3.3'], REAL '0.0')", new ArrayType(REAL), ImmutableList.of(1.0f, 2.0f, 3.3f));

        // Test with negative element.
        assertFunction("array_normalize(ARRAY[-1.0E0, 2.0E0, 3.3E0], 0.0E0)", new ArrayType(DOUBLE), ImmutableList.of(-1.0, 2.0, 3.3));
        assertFunction("array_normalize(ARRAY[REAL '-1.0', REAL '2.0', REAL '3.3'], REAL '0.0')", new ArrayType(REAL), ImmutableList.of(-1.0f, 2.0f, 3.3f));
    }

    @Test
    public void test1Norm()
    {
        assertFunction("array_normalize(ARRAY[1.0E0, 2.0E0, 3.0E0], 1.0E0)", new ArrayType(DOUBLE), ImmutableList.of(1.0 / 6.0, 2.0 / 6.0, 3.0 / 6.0));
        assertFunction("array_normalize(ARRAY[REAL '1.0', REAL '2.0', REAL '3.0'], REAL '1.0')", new ArrayType(REAL), ImmutableList.of(1.0f / 6.0f, 2.0f / 6.0f, 3.0f / 6.0f));

        // Test with negative element.
        assertFunction("array_normalize(ARRAY[-1.0E0, 2.0E0, 3.0E0], 1.0E0)", new ArrayType(DOUBLE), ImmutableList.of(-1.0 / 6.0, 2.0 / 6.0, 3.0 / 6.0));
        assertFunction("array_normalize(ARRAY[REAL '-1.0', REAL '2.0', REAL '3.0'], REAL '1.0')", new ArrayType(REAL), ImmutableList.of(-1.0f / 6.0f, 2.0f / 6.0f, 3.0f / 6.0f));
    }

    @Test
    public void test2Norm()
    {
        assertFunction("array_normalize(ARRAY[4.0E0, 3.0E0], 2.0E0)", new ArrayType(DOUBLE), ImmutableList.of(4.0 / 5.0, 3.0 / 5.0));
        assertFunction("array_normalize(ARRAY[REAL '4.0', REAL '3.0'], REAL '2.0')", new ArrayType(REAL), ImmutableList.of(4.0f / 5.0f, 3.0f / 5.0f));

        // Test with negative element.
        assertFunction("array_normalize(ARRAY[-4.0E0, 3.0E0], 2.0E0)", new ArrayType(DOUBLE), ImmutableList.of(-4.0 / 5.0, 3.0 / 5.0));
        assertFunction("array_normalize(ARRAY[REAL '-4.0', REAL '3.0'], REAL '2.0')", new ArrayType(REAL), ImmutableList.of(-4.0f / 5.0f, 3.0f / 5.0f));
    }

    @Test
    public void testNulls()
    {
        assertFunction("array_normalize(null, 2.0E0)", new ArrayType(DOUBLE), null);
        assertFunction("array_normalize(ARRAY[4.0E0, 3.0E0], null)", new ArrayType(DOUBLE), null);
        assertFunction("array_normalize(ARRAY[4.0E0, null], 2.0E0)", new ArrayType(DOUBLE), null);
        assertFunction("array_normalize(ARRAY[REAL '4.0', REAL '3.0'], null)", new ArrayType(REAL), null);
    }

    @Test
    public void testArrayOfZeros()
    {
        assertFunction("array_normalize(ARRAY[0.0E0, 0.0E0], 1.0E0)", new ArrayType(DOUBLE), ImmutableList.of(0.0, 0.0));
        assertFunction("array_normalize(ARRAY[REAL '0.0', REAL '0.0'], REAL '1.0')", new ArrayType(REAL), ImmutableList.of(0.0f, 0.0f));

        assertFunction("array_normalize(ARRAY[0.0E0, 0.0E0], 2.0E0)", new ArrayType(DOUBLE), ImmutableList.of(0.0, 0.0));
        assertFunction("array_normalize(ARRAY[REAL '0.0', REAL '0.0'], REAL '2.0')", new ArrayType(REAL), ImmutableList.of(0.0f, 0.0f));
    }

    @Test
    public void testUnsupportedType()
    {
        assertInvalidFunction(
                "array_normalize(ARRAY[1, 2, 3], 1)",
                FUNCTION_IMPLEMENTATION_MISSING,
                "Unsupported array element type for array_normalize function: integer");
        assertInvalidFunction(
                "array_normalize(ARRAY['a', 'b', 'c'], 'd')",
                FUNCTION_IMPLEMENTATION_MISSING,
                "Unsupported type parameters.*");
    }

    @Test
    public void testNegativeP()
    {
        assertInvalidFunction(
                "array_normalize(ARRAY[1.0E0, 2.0E0, 3.3E0], -1.0E0)",
                INVALID_FUNCTION_ARGUMENT,
                "array_normalize only supports non-negative p:.*");
        assertInvalidFunction(
                "array_normalize(ARRAY[REAL '1.0', REAL '2.0', REAL '3.3'], REAL '-1.0')",
                INVALID_FUNCTION_ARGUMENT,
                "array_normalize only supports non-negative p:.*");
    }
}
