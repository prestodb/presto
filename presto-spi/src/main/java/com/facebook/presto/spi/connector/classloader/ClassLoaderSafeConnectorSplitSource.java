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

import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorSplitSource
        implements ConnectorSplitSource
{
    private final ConnectorSplitSource delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorSplitSource(ConnectorSplitSource delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getNextBatch(partitionHandle, maxSize);
        }
    }

    @Override
    public void close()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.close();
        }
    }

    @Override
    public boolean isFinished()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.isFinished();
        }
    }
}
