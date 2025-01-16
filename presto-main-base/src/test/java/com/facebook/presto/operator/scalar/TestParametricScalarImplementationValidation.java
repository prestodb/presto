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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.util.Reflection.methodHandle;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestParametricScalarImplementationValidation
{
    private static final MethodHandle STATE_FACTORY = methodHandle(TestParametricScalarImplementationValidation.class, "createState");

    @Test
    public void testSqlFunctionPropertiesPosition()
    {
        // Without cached instance factory
        MethodHandle validFunctionMethodHandle = methodHandle(TestParametricScalarImplementationValidation.class, "validSqlFunctionPropertiesParameterPosition", SqlFunctionProperties.class, long.class, long.class);
        BuiltInScalarFunctionImplementation validFunction = new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                validFunctionMethodHandle);
        assertEquals(validFunction.getMethodHandle(), validFunctionMethodHandle);

        try {
            BuiltInScalarFunctionImplementation invalidFunction = new BuiltInScalarFunctionImplementation(
                    false,
                    ImmutableList.of(
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    methodHandle(TestParametricScalarImplementationValidation.class, "invalidSqlFunctionPropertiesParameterPosition", long.class, long.class, SqlFunctionProperties.class));
            fail("expected exception");
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "SqlFunctionProperties must be the first argument when instanceFactory is not present");
        }

        // With cached instance factory
        MethodHandle validFunctionWithInstanceFactoryMethodHandle = methodHandle(TestParametricScalarImplementationValidation.class, "validSqlFunctionPropertiesParameterPosition", Object.class, SqlFunctionProperties.class, long.class, long.class);
        BuiltInScalarFunctionImplementation validFunctionWithInstanceFactory = new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                validFunctionWithInstanceFactoryMethodHandle,
                Optional.of(STATE_FACTORY));
        assertEquals(validFunctionWithInstanceFactory.getMethodHandle(), validFunctionWithInstanceFactoryMethodHandle);

        try {
            BuiltInScalarFunctionImplementation invalidFunctionWithInstanceFactory = new BuiltInScalarFunctionImplementation(
                    false,
                    ImmutableList.of(
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    methodHandle(TestParametricScalarImplementationValidation.class, "invalidSqlFunctionPropertiesParameterPosition", Object.class, long.class, long.class, SqlFunctionProperties.class),
                    Optional.of(STATE_FACTORY));
            fail("expected exception");
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "SqlFunctionProperties must be the second argument when instanceFactory is present");
        }
    }

    public static Object createState()
    {
        return null;
    }

    public static long validSqlFunctionPropertiesParameterPosition(SqlFunctionProperties properties, long arg1, long arg2)
    {
        return arg1 + arg2;
    }

    public static long validSqlFunctionPropertiesParameterPosition(Object state, SqlFunctionProperties properties, long arg1, long arg2)
    {
        return arg1 + arg2;
    }

    public static long invalidSqlFunctionPropertiesParameterPosition(long arg1, long arg2, SqlFunctionProperties properties)
    {
        return arg1 + arg2;
    }

    public static long invalidSqlFunctionPropertiesParameterPosition(Object state, long arg1, long arg2, SqlFunctionProperties properties)
    {
        return arg1 + arg2;
    }
}
