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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.Index;
import com.facebook.presto.spi.IndexHandle;
import com.facebook.presto.spi.ResolvedIndex;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorIndexResolver
        implements ConnectorIndexResolver
{
    private final ConnectorIndexResolver delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorIndexResolver(ConnectorIndexResolver delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(tableHandle);
        }
    }

    @Override
    public boolean canHandle(IndexHandle indexHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(indexHandle);
        }
    }

    @Override
    public ResolvedIndex resolveIndex(TableHandle tableHandle, Set<ColumnHandle> indexableColumns, TupleDomain tupleDomain)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.resolveIndex(tableHandle, indexableColumns, tupleDomain);
        }
    }

    @Override
    public Index getIndex(IndexHandle indexHandle, List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getIndex(indexHandle, lookupSchema, outputSchema);
        }
    }
}
