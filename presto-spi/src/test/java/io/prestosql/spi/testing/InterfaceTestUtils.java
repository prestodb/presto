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
package io.prestosql.spi.testing;

import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Method;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public final class InterfaceTestUtils
{
    private InterfaceTestUtils() {}

    public static <I, C extends I> void assertAllMethodsOverridden(Class<I> iface, Class<C> clazz)
    {
        assertEquals(ImmutableSet.copyOf(clazz.getInterfaces()), ImmutableSet.of(iface));
        for (Method method : iface.getMethods()) {
            try {
                Method override = clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
                assertEquals(override.getReturnType(), method.getReturnType());
            }
            catch (NoSuchMethodException e) {
                fail(format("%s does not override [%s]", clazz.getName(), method));
            }
        }
    }
}
