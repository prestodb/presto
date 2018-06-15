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
package com.facebook.presto.hive.security;

import com.facebook.presto.spi.connector.ConnectorAccessControl;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static java.lang.String.format;

public class TestPartitionsAwareAccessControl
{
    @Test
    public void testEverythingDelegated()
            throws Exception
    {
        checkEverythingImplemented(ConnectorAccessControl.class, PartitionsAwareAccessControl.class);
    }

    private static <I> void checkEverythingImplemented(Class<I> interfaceClass, Class<? extends I> implementationClass)
            throws ReflectiveOperationException
    {
        for (Method interfaceMethod : interfaceClass.getMethods()) {
            Method implementationMethod = implementationClass.getMethod(interfaceMethod.getName(), interfaceMethod.getParameterTypes());
            if (interfaceMethod.equals(implementationMethod) && interfaceMethod.getAnnotation(Deprecated.class) == null) {
                throw new AssertionError(format(
                        "Method should be overridden in %s: %s",
                        implementationClass,
                        interfaceMethod));
            }
        }
    }
}
