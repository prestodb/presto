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
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSession;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PageSourceManager
        implements PageSourceProvider
{
    private final ConcurrentMap<String, ConnectorPageSourceProvider> pageSourceProviders = new ConcurrentHashMap<>();

    public void addConnectorPageSourceProvider(String connectorId, ConnectorPageSourceProvider connectorPageSourceProvider)
    {
        pageSourceProviders.put(connectorId, connectorPageSourceProvider);
    }

    @Override
    public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");

        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(split.getConnectorId());
        return getPageSourceProvider(split).createPageSource(connectorSession, split.getConnectorSplit(), columns);
    }

    private ConnectorPageSourceProvider getPageSourceProvider(Split split)
    {
        ConnectorPageSourceProvider provider = pageSourceProviders.get(split.getConnectorId());

        checkArgument(provider != null, "No page stream provider for '%s", split.getConnectorId());

        return provider;
    }
}
