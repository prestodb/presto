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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorHandleResolver
        implements ConnectorHandleResolver
{
    private final ConnectorHandleResolver delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorHandleResolver(ConnectorHandleResolver delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(tableHandle);
        }
    }

    @Override
    public boolean canHandle(ConnectorColumnHandle columnHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(columnHandle);
        }
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(split);
        }
    }

    @Override
    public boolean canHandle(ConnectorIndexHandle indexHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(indexHandle);
        }
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableHandleClass();
        }
    }

    @Override
    public Class<? extends ConnectorColumnHandle> getColumnHandleClass()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnHandleClass();
        }
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSplitClass();
        }
    }

    @Override
    public String toString()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.toString();
        }
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getIndexHandleClass();
        }
    }
}
