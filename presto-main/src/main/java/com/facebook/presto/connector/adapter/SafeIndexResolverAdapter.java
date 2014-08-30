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
package com.facebook.presto.connector.adapter;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Index;
import com.facebook.presto.spi.SafeIndexResolver;
import com.facebook.presto.spi.TupleDomain;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class SafeIndexResolverAdapter<TH extends ConnectorTableHandle, CH extends ConnectorColumnHandle, IH extends ConnectorIndexHandle>
        implements ConnectorIndexResolver
{
    private final SafeIndexResolver<TH, CH, IH> delegate;
    private final SafeTypeAdapter<TH, CH, ?, IH, ?, ?, ?> typeAdapter;

    public SafeIndexResolverAdapter(SafeIndexResolver<TH, CH, IH> delegate, SafeTypeAdapter<TH, CH, ?, IH, ?, ?, ?> typeAdapter)
    {
        this.delegate = checkNotNull(delegate, "delegate is null");
        this.typeAdapter = checkNotNull(typeAdapter, "typeAdapter is null");
    }

    @Override
    public ConnectorResolvedIndex resolveIndex(ConnectorTableHandle tableHandle, Set<ConnectorColumnHandle> indexableColumns, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        return delegate.resolveIndex(typeAdapter.castTableHandle(tableHandle), typeAdapter.castColumnHandlesSet(indexableColumns), typeAdapter.castTupleDomain(tupleDomain));
    }

    @Override
    public Index getIndex(ConnectorIndexHandle indexHandle, List<ConnectorColumnHandle> lookupSchema, List<ConnectorColumnHandle> outputSchema)
    {
        return delegate.getIndex(typeAdapter.castIndexHandle(indexHandle), typeAdapter.castColumnHandles(lookupSchema), typeAdapter.castColumnHandles(outputSchema));
    }
}
