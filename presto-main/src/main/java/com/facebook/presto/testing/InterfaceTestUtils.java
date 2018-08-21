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
package com.facebook.presto.testing;

import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class InterfaceTestUtils
{
    private InterfaceTestUtils() {}

    // Note: this should be in sync with com.facebook.presto.plugin.base.testing.InterfaceTestUtils#checkEverythingImplemented
    public static <I> void checkEverythingImplemented(Class<I> interfaceClass, Class<? extends I> implementationClass)
            throws ReflectiveOperationException
    {
        checkArgument(interfaceClass.isAssignableFrom(implementationClass));
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
