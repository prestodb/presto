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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PageSourceManager
        implements PageSourceProvider
{
    private final ConcurrentMap<ConnectorId, ConnectorPageSourceProvider> pageSourceProviders = new ConcurrentHashMap<>();

    public void addConnectorPageSourceProvider(ConnectorId connectorId, ConnectorPageSourceProvider connectorPageSourceProvider)
    {
        pageSourceProviders.put(connectorId, connectorPageSourceProvider);
    }

    @Override
    public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        ConnectorSession connectorSession = session.toConnectorSession(split.getConnectorId());
        return getPageSourceProvider(split).createPageSource(split.getTransactionHandle(), connectorSession, split.getConnectorSplit(), columns);
    }

    private ConnectorPageSourceProvider getPageSourceProvider(Split split)
    {
        ConnectorPageSourceProvider provider = pageSourceProviders.get(split.getConnectorId());

        checkArgument(provider != null, "No page stream provider for '%s", split.getConnectorId());

        return provider;
    }
}
