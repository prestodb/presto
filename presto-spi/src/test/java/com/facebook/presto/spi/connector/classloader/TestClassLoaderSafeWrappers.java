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
package com.facebook.presto.spi.connector.classloader;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorPageSink;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestClassLoaderSafeWrappers
{
    @Test
    public void testAllMethodsOverridden()
    {
        assertAllMethodsOverridden(ConnectorMetadata.class, ClassLoaderSafeConnectorMetadata.class);
        assertAllMethodsOverridden(ConnectorPageSink.class, ClassLoaderSafeConnectorPageSink.class);
        assertAllMethodsOverridden(ConnectorPageSinkProvider.class, ClassLoaderSafeConnectorPageSinkProvider.class);
        assertAllMethodsOverridden(ConnectorPageSourceProvider.class, ClassLoaderSafeConnectorPageSourceProvider.class);
        assertAllMethodsOverridden(ConnectorSplitManager.class, ClassLoaderSafeConnectorSplitManager.class);
        assertAllMethodsOverridden(ConnectorNodePartitioningProvider.class, ClassLoaderSafeNodePartitioningProvider.class);
    }

    private static <I, C extends I> void assertAllMethodsOverridden(Class<I> iface, Class<C> clazz)
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
