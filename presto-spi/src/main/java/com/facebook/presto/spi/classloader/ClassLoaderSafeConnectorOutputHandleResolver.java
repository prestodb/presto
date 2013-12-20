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
package com.facebook.presto.spi.classloader;

import com.facebook.presto.spi.ConnectorOutputHandleResolver;
import com.facebook.presto.spi.OutputTableHandle;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorOutputHandleResolver
        implements ConnectorOutputHandleResolver
{
    private final ConnectorOutputHandleResolver delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorOutputHandleResolver(ConnectorOutputHandleResolver delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public boolean canHandle(OutputTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(tableHandle);
        }
    }

    @Override
    public Class<? extends OutputTableHandle> getOutputTableHandleClass()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getOutputTableHandleClass();
        }
    }
}
