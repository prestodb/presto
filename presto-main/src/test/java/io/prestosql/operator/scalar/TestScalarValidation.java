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
package io.prestosql.operator.scalar;

import io.prestosql.metadata.FunctionListBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
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

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Parametric class .* does not have any annotated methods")
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

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Method .* annotated with @SqlType is missing @ScalarFunction or @ScalarOperator")
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

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* has wrapper return type Long but is missing @SqlNullable")
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

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* annotated with @SqlNullable has primitive return type long")
    public void testPrimitiveReturnWithNullable()
    {
        extractScalars(PrimitiveReturnWithNullable.class);
    }

    public static final class PrimitiveReturnWithNullable
    {
        @ScalarFunction
        @SqlNullable
        @SqlType(StandardTypes.BIGINT)
        public static long bad()
        {
            return 0L;
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "A parameter with USE_NULL_FLAG or RETURN_NULL_ON_NULL convention must not use wrapper type. Found in method .*")
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

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Method .* has parameter with primitive type double annotated with @SqlNullable")
    public void testPrimitiveParameterWithNullable()
    {
        extractScalars(PrimitiveParameterWithNullable.class);
    }

    public static final class PrimitiveParameterWithNullable
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlNullable @SqlType(StandardTypes.DOUBLE) double primitive)
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

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* is annotated with @Nullable but not @SqlNullable")
    public void testMethodWithLegacyNullable()
    {
        extractScalars(MethodWithLegacyNullable.class);
    }

    public static final class MethodWithLegacyNullable
    {
        @ScalarFunction
        @Nullable
        @SqlType(StandardTypes.BIGINT)
        public static Long bad()
        {
            return 0L;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* has @IsNull parameter that does not follow a @SqlType parameter")
    public void testParameterWithConnectorAndIsNull()
    {
        extractScalars(ParameterWithConnectorAndIsNull.class);
    }

    public static final class ParameterWithConnectorAndIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(ConnectorSession session, @IsNull boolean isNull)
        {
            return 0;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* has @IsNull parameter that does not follow a @SqlType parameter")
    public void testParameterWithOnlyIsNull()
    {
        extractScalars(ParameterWithOnlyIsNull.class);
    }

    public static final class ParameterWithOnlyIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@IsNull boolean isNull)
        {
            return 0;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* has non-boolean parameter with @IsNull")
    public void testParameterWithNonBooleanIsNull()
    {
        extractScalars(ParameterWithNonBooleanIsNull.class);
    }

    public static final class ParameterWithNonBooleanIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlType(StandardTypes.BIGINT) long value, @IsNull int isNull)
        {
            return 0;
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "A parameter with USE_NULL_FLAG or RETURN_NULL_ON_NULL convention must not use wrapper type. Found in method .*")
    public void testParameterWithBoxedPrimitiveIsNull()
    {
        extractScalars(ParameterWithBoxedPrimitiveIsNull.class);
    }

    public static final class ParameterWithBoxedPrimitiveIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlType(StandardTypes.BIGINT) Long value, @IsNull boolean isNull)
        {
            return 0;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Method .* has @IsNull parameter that has other annotations")
    public void testParameterWithOtherAnnotationsWithIsNull()
    {
        extractScalars(ParameterWithOtherAnnotationsWithIsNull.class);
    }

    public static final class ParameterWithOtherAnnotationsWithIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlType(StandardTypes.BIGINT) long value, @IsNull @SqlNullable boolean isNull)
        {
            return 0;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Expected type parameter to only contain A-Z and 0-9 \\(starting with A-Z\\), but got bad on method .*")
    public void testNonUpperCaseTypeParameters()
    {
        extractScalars(TypeParameterWithNonUpperCaseAnnotation.class);
    }

    public static final class TypeParameterWithNonUpperCaseAnnotation
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("bad")
        public static long bad(@TypeParameter("array(bad)") Type type, @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Expected type parameter to only contain A-Z and 0-9 \\(starting with A-Z\\), but got 1E on method .*")
    public void testLeadingNumericTypeParameters()
    {
        extractScalars(TypeParameterWithLeadingNumbers.class);
    }

    public static final class TypeParameterWithLeadingNumbers
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("1E")
        public static long bad(@TypeParameter("array(1E)") Type type, @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Expected type parameter not to take parameters, but got E on method .*")
    public void testNonPrimitiveTypeParameters()
    {
        extractScalars(TypeParameterWithNonPrimitiveAnnotation.class);
    }

    public static final class TypeParameterWithNonPrimitiveAnnotation
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("E")
        public static long bad(@TypeParameter("E(VARCHAR)") Type type, @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testValidTypeParameters()
    {
        extractScalars(ValidTypeParameter.class);
    }

    public static final class ValidTypeParameter
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long good1(
                @TypeParameter("ROW(ARRAY(BIGINT),MAP(INTEGER,DECIMAL),SMALLINT,CHAR,BOOLEAN,DATE,TIMESTAMP,VARCHAR)") Type type,
                @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }

        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("E12")
        @TypeParameter("F34")
        public static long good2(
                @TypeParameter("ROW(ARRAY(E12),JSON,TIME,VARBINARY,ROW(ROW(F34)))") Type type,
                @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testValidTypeParametersForConstructors()
    {
        extractParametricScalar(ConstructorWithValidTypeParameters.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Expected type parameter not to take parameters, but got K on method .*")
    public void testInvalidTypeParametersForConstructors()
    {
        extractParametricScalar(ConstructorWithInvalidTypeParameters.class);
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
