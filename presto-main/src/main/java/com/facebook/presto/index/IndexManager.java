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

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.IndexHandle;
import com.facebook.presto.metadata.ResolvedIndex;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.Index;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.metadata.ColumnHandle.connectorHandleGetter;
import static com.facebook.presto.metadata.Util.toConnectorDomain;

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

    public Optional<ResolvedIndex> resolveIndex(TableHandle tableHandle, Set<ColumnHandle> indexableColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        Optional<ConnectorIndexResolver> resolver = getConnectorIndexResolver(tableHandle);
        if (!resolver.isPresent()) {
            return Optional.absent();
        }

        Set<ConnectorColumnHandle> columns = ImmutableSet.copyOf(Iterables.transform(indexableColumns, ColumnHandle.connectorHandleGetter()));
        ConnectorResolvedIndex resolved = resolver.get().resolveIndex(tableHandle.getConnectorHandle(), columns, toConnectorDomain(tupleDomain));

        if (resolved == null) {
            return Optional.absent();
        }

        return Optional.of(new ResolvedIndex(tableHandle.getConnectorId(), resolved));
    }

    public Index getIndex(IndexHandle indexHandle, List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema)
    {
        return getConnectorIndexResolver(indexHandle)
                .getIndex(indexHandle.getConnectorHandle(), Lists.transform(lookupSchema, connectorHandleGetter()), Lists.transform(outputSchema, connectorHandleGetter()));
    }

    private Optional<ConnectorIndexResolver> getConnectorIndexResolver(TableHandle handle)
    {
        for (ConnectorIndexResolver indexResolver : indexResolvers) {
            if (indexResolver.canHandle(handle.getConnectorHandle())) {
                return Optional.of(indexResolver);
            }
        }
        return Optional.absent();
    }

    private ConnectorIndexResolver getConnectorIndexResolver(IndexHandle handle)
    {
        for (ConnectorIndexResolver resolver : indexResolvers) {
            if (resolver.canHandle(handle.getConnectorHandle())) {
                return resolver;
            }
        }
        throw new IllegalArgumentException("Not index resolver for " + handle);
    }
}
