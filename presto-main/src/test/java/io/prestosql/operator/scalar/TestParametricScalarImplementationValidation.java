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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.util.Reflection.methodHandle;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestParametricScalarImplementationValidation
{
    private static final MethodHandle STATE_FACTORY = methodHandle(TestParametricScalarImplementationValidation.class, "createState");

    @Test
    public void testConnectorSessionPosition()
    {
        // Without cached instance factory
        MethodHandle validFunctionMethodHandle = methodHandle(TestParametricScalarImplementationValidation.class, "validConnectorSessionParameterPosition", ConnectorSession.class, long.class, long.class);
        ScalarFunctionImplementation validFunction = new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                validFunctionMethodHandle,
                false);
        assertEquals(validFunction.getMethodHandle(), validFunctionMethodHandle);

        try {
            ScalarFunctionImplementation invalidFunction = new ScalarFunctionImplementation(
                    false,
                    ImmutableList.of(
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    methodHandle(TestParametricScalarImplementationValidation.class, "invalidConnectorSessionParameterPosition", long.class, long.class, ConnectorSession.class),
                    false);
            fail("expected exception");
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "ConnectorSession must be the first argument when instanceFactory is not present");
        }

        // With cached instance factory
        MethodHandle validFunctionWithInstanceFactoryMethodHandle = methodHandle(TestParametricScalarImplementationValidation.class, "validConnectorSessionParameterPosition", Object.class, ConnectorSession.class, long.class, long.class);
        ScalarFunctionImplementation validFunctionWithInstanceFactory = new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                validFunctionWithInstanceFactoryMethodHandle,
                Optional.of(STATE_FACTORY),
                false);
        assertEquals(validFunctionWithInstanceFactory.getMethodHandle(), validFunctionWithInstanceFactoryMethodHandle);

        try {
            ScalarFunctionImplementation invalidFunctionWithInstanceFactory = new ScalarFunctionImplementation(
                    false,
                    ImmutableList.of(
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    methodHandle(TestParametricScalarImplementationValidation.class, "invalidConnectorSessionParameterPosition", Object.class, long.class, long.class, ConnectorSession.class),
                    Optional.of(STATE_FACTORY),
                    false);
            fail("expected exception");
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "ConnectorSession must be the second argument when instanceFactory is present");
        }
    }

    public static Object createState()
    {
        return null;
    }

    public static long validConnectorSessionParameterPosition(ConnectorSession session, long arg1, long arg2)
    {
        return arg1 + arg2;
    }

    public static long validConnectorSessionParameterPosition(Object state, ConnectorSession session, long arg1, long arg2)
    {
        return arg1 + arg2;
    }

    public static long invalidConnectorSessionParameterPosition(long arg1, long arg2, ConnectorSession session)
    {
        return arg1 + arg2;
    }

    public static long invalidConnectorSessionParameterPosition(Object state, long arg1, long arg2, ConnectorSession session)
    {
        return arg1 + arg2;
    }
}
