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
package com.facebook.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.connector.CatalogName;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PageSourceManager
        implements PageSourceProvider
{
    private final ConcurrentMap<CatalogName, ConnectorPageSourceProvider> pageSourceProviders = new ConcurrentHashMap<>();

    public void addConnectorPageSourceProvider(CatalogName catalogName, ConnectorPageSourceProvider pageSourceProvider)
    {
        requireNonNull(catalogName, "connectorId is null");
        requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        checkState(pageSourceProviders.put(catalogName, pageSourceProvider) == null, "PageSourceProvider for connector '%s' is already registered", catalogName);
    }

    public void removeConnectorPageSourceProvider(CatalogName catalogName)
    {
        pageSourceProviders.remove(catalogName);
    }

    @Override
    public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        ConnectorSession connectorSession = session.toConnectorSession(split.getCatalogName());
        return getPageSourceProvider(split).createPageSource(split.getTransactionHandle(), connectorSession, split.getConnectorSplit(), columns);
    }

    private ConnectorPageSourceProvider getPageSourceProvider(Split split)
    {
        ConnectorPageSourceProvider provider = pageSourceProviders.get(split.getCatalogName());

        checkArgument(provider != null, "No page stream provider for '%s", split.getCatalogName());

        return provider;
    }
}
