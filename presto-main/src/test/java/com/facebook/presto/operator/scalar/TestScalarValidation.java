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

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

@SuppressWarnings("UtilityClassWithoutPrivateConstructor")
public class TestScalarValidation
{
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Parametric class method .* is annotated with @ScalarFunction")
    public void testBogusParametricMethodAnnotation()
    {
        extractParametricScalar(BogusParametricMethodAnnotation.class);
    }

    @ScalarFunction
    public static final class BogusParametricMethodAnnotation
    {
        @ScalarFunction
        public static void bad() {}
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Parametric class .* does not have any annotated methods")
    public void testNoParametricMethods()
    {
        extractParametricScalar(NoParametricMethods.class);
    }

    @SuppressWarnings("EmptyClass")
    @ScalarFunction
    public static final class NoParametricMethods {}

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* is missing @SqlType annotation")
    public void testMethodMissingReturnAnnotation()
    {
        extractScalars(MethodMissingReturnAnnotation.class);
    }

    public static final class MethodMissingReturnAnnotation
    {
        @ScalarFunction
        public static void bad() {}
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* annotated with @SqlType is missing @ScalarFunction or @ScalarOperator")
    public void testMethodMissingScalarAnnotation()
    {
        extractScalars(MethodMissingScalarAnnotation.class);
    }

    public static final class MethodMissingScalarAnnotation
    {
        @SuppressWarnings("unused")
        @SqlType
        public static void bad() {}
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* has wrapper return type Long but is missing @Nullable")
    public void testPrimitiveWrapperReturnWithoutNullable()
    {
        extractScalars(PrimitiveWrapperReturnWithoutNullable.class);
    }

    public static final class PrimitiveWrapperReturnWithoutNullable
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static Long bad()
        {
            return 0L;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* annotated with @Nullable has primitive return type long")
    public void testPrimitiveReturnWithNullable()
    {
        extractScalars(PrimitiveReturnWithNullable.class);
    }

    public static final class PrimitiveReturnWithNullable
    {
        @SuppressWarnings("NullableProblems")
        @ScalarFunction
        @Nullable
        @SqlType(StandardTypes.BIGINT)
        public static long bad()
        {
            return 0L;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* has parameter with wrapper type Boolean that is missing @Nullable")
    public void testPrimitiveWrapperParameterWithoutNullable()
    {
        extractScalars(PrimitiveWrapperParameterWithoutNullable.class);
    }

    public static final class PrimitiveWrapperParameterWithoutNullable
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlType(StandardTypes.BOOLEAN) Boolean boxed)
        {
            return 0;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* has parameter with primitive type double annotated with @Nullable")
    public void testPrimitiveParameterWithNullable()
    {
        extractScalars(PrimitiveParameterWithNullable.class);
    }

    public static final class PrimitiveParameterWithNullable
    {
        @SuppressWarnings("NullableProblems")
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@Nullable @SqlType(StandardTypes.DOUBLE) double primitive)
        {
            return 0;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* is missing @SqlType annotation for parameter")
    public void testParameterWithoutType()
    {
        extractScalars(ParameterWithoutType.class);
    }

    public static final class ParameterWithoutType
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(long missing)
        {
            return 0;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* annotated with @ScalarFunction must be public")
    public void testNonPublicAnnnotatedMethod()
    {
        extractScalars(NonPublicAnnnotatedMethod.class);
    }

    public static final class NonPublicAnnnotatedMethod
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        private static long bad()
        {
            return 0;
        }
    }

    private static void extractParametricScalar(Class<?> clazz)
    {
        new FunctionListBuilder().scalar(clazz);
    }

    private static void extractScalars(Class<?> clazz)
    {
        new FunctionListBuilder().scalars(clazz);
    }
}
