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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;

public class TestScalarParser
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerParametricScalar(GenericWithIncompleteSpecializationNullable.class);
        registerParametricScalar(GenericWithIncompleteSpecializationNotNullable.class);
    }

    @Test
    public void testGenericWithIncompleteSpecialization()
    {
        assertFunction("generic_incomplete_specialization_nullable(9876543210)", BIGINT, 9876543210L);
        assertFunction("generic_incomplete_specialization_nullable(1.234E0)", DOUBLE, 1.234);
        assertFunction("generic_incomplete_specialization_nullable('abcd')", createVarcharType(4), "abcd");
        assertFunction("generic_incomplete_specialization_nullable(true)", BOOLEAN, true);
        assertFunction("generic_incomplete_specialization_nullable(array[1, 2])", new ArrayType(INTEGER), ImmutableList.of(1, 2));

        assertFunction("generic_incomplete_specialization_not_nullable(9876543210)", BIGINT, 9876543210L);
        assertFunction("generic_incomplete_specialization_not_nullable(1.234E0)", DOUBLE, 1.234);
        assertFunction("generic_incomplete_specialization_not_nullable('abcd')", createVarcharType(4), "abcd");
        assertFunction("generic_incomplete_specialization_not_nullable(true)", BOOLEAN, true);
        assertFunction("generic_incomplete_specialization_not_nullable(array[1, 2])", new ArrayType(INTEGER), ImmutableList.of(1, 2));
    }

    @ScalarFunction(value = "generic_incomplete_specialization_nullable", calledOnNullInput = true)
    public static class GenericWithIncompleteSpecializationNullable
    {
        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Object generic(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Object input)
        {
            return input;
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Long specializedSlice(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Long input)
        {
            return input;
        }
    }

    @ScalarFunction("generic_incomplete_specialization_not_nullable")
    public static class GenericWithIncompleteSpecializationNotNullable
    {
        @TypeParameter("E")
        @SqlType("E")
        public static Object generic(@TypeParameter("E") Type type, @SqlType("E") Object input)
        {
            return input;
        }

        @TypeParameter("E")
        @SqlType("E")
        public static long specializedSlice(@TypeParameter("E") Type type, @SqlType("E") long input)
        {
            return input;
        }
    }
}
