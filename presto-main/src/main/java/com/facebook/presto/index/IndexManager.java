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

import com.facebook.presto.Session;
import com.facebook.presto.connector.CatalogName;
import com.facebook.presto.metadata.IndexHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IndexManager
{
    private final ConcurrentMap<CatalogName, ConnectorIndexProvider> providers = new ConcurrentHashMap<>();

    public void addIndexProvider(CatalogName catalogName, ConnectorIndexProvider indexProvider)
    {
        requireNonNull(catalogName, "connectorId is null");
        requireNonNull(indexProvider, "indexProvider is null");
        checkState(providers.putIfAbsent(catalogName, indexProvider) == null, "IndexProvider for connector '%s' is already registered", catalogName);
    }

    public void removeIndexProvider(CatalogName catalogName)
    {
        providers.remove(catalogName);
    }

    public ConnectorIndex getIndex(Session session, IndexHandle indexHandle, List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema)
    {
        ConnectorSession connectorSession = session.toConnectorSession(indexHandle.getCatalogName());
        ConnectorIndexProvider provider = getProvider(indexHandle);
        return provider.getIndex(indexHandle.getTransactionHandle(), connectorSession, indexHandle.getConnectorHandle(), lookupSchema, outputSchema);
    }

    private ConnectorIndexProvider getProvider(IndexHandle handle)
    {
        ConnectorIndexProvider result = providers.get(handle.getCatalogName());
        checkArgument(result != null, "No index provider for connector '%s'", handle.getCatalogName());
        return result;
    }
}
