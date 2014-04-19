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
package com.facebook.presto.index;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.Index;
import com.facebook.presto.spi.IndexHandle;
import com.facebook.presto.spi.ResolvedIndex;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IndexManager
{
    private final Set<ConnectorIndexResolver> indexResolvers = Sets.newSetFromMap(new ConcurrentHashMap<ConnectorIndexResolver, Boolean>());

    @Inject
    public IndexManager(Set<ConnectorIndexResolver> indexResolvers)
    {
        this.indexResolvers.addAll(indexResolvers);
    }

    public IndexManager()
    {
    }

    public void addIndexResolver(ConnectorIndexResolver connectorIndexResolver)
    {
        indexResolvers.add(connectorIndexResolver);
    }

    public Optional<ResolvedIndex> resolveIndex(TableHandle tableHandle, Set<ColumnHandle> indexableColumns, TupleDomain tupleDomain)
    {
        Optional<ConnectorIndexResolver> connectorIndexResolver = getConnectorIndexResolver(tableHandle);
        if (!connectorIndexResolver.isPresent()) {
            return Optional.absent();
        }
        return Optional.fromNullable(connectorIndexResolver.get().resolveIndex(tableHandle, indexableColumns, tupleDomain));
    }

    public Index getIndex(IndexHandle indexHandle, List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema)
    {
        return getConnectorIndexResolver(indexHandle).getIndex(indexHandle, lookupSchema, outputSchema);
    }

    private Optional<ConnectorIndexResolver> getConnectorIndexResolver(TableHandle handle)
    {
        for (ConnectorIndexResolver indexResolver : indexResolvers) {
            if (indexResolver.canHandle(handle)) {
                return Optional.of(indexResolver);
            }
        }
        return Optional.absent();
    }

    private ConnectorIndexResolver getConnectorIndexResolver(IndexHandle handle)
    {
        for (ConnectorIndexResolver indexResolver : indexResolvers) {
            if (indexResolver.canHandle(handle)) {
                return indexResolver;
            }
        }
        throw new IllegalArgumentException("No ConnectorIndexResolver found for IndexHandle: " + handle);
    }
}
