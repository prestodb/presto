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

import com.facebook.presto.common.Page;
import com.facebook.presto.spi.ConnectorMergeSink;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorMergeSink
        implements ConnectorMergeSink
{
    private final ConnectorMergeSink delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorMergeSink(ConnectorMergeSink delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public void storeMergedRows(Page page)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.storeMergedRows(page);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.finish();
        }
    }

    @Override
    public void abort()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.abort();
        }
    }
}
