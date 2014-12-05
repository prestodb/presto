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

import com.facebook.presto.connector.system.SystemRecordSetProvider;
import com.facebook.presto.connector.system.SystemTablesManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.google.common.collect.Lists;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PageSourceManager
        implements PageSourceProvider
{
    private final ConcurrentMap<String, ConnectorPageSourceProvider> pageSourceProviders = new ConcurrentHashMap<>();

    @Inject
    public PageSourceManager(SystemRecordSetProvider systemRecordSetProvider)
    {
        pageSourceProviders.put(SystemTablesManager.CONNECTOR_ID, new RecordPageSourceProvider(systemRecordSetProvider));
    }

    public void addConnectorPageSourceProvider(String connectorId, ConnectorPageSourceProvider connectorPageSourceProvider)
    {
        pageSourceProviders.put(connectorId, connectorPageSourceProvider);
    }

    @Override
    public ConnectorPageSource createPageSource(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");

        List<ConnectorColumnHandle> handles = Lists.transform(columns, ColumnHandle::getConnectorHandle);

        return getPageSourceProvider(split).createPageSource(split.getConnectorSplit(), handles);
    }

    private ConnectorPageSourceProvider getPageSourceProvider(Split split)
    {
        ConnectorPageSourceProvider provider = pageSourceProviders.get(split.getConnectorId());

        checkArgument(provider != null, "No page stream provider for '%s", split.getConnectorId());

        return provider;
    }
}
