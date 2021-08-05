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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PageSourceManager
        implements PageSourceProvider
{
    private final ConcurrentMap<ConnectorId, ConnectorPageSourceProvider> pageSourceProviders = new ConcurrentHashMap<>();

    public void addConnectorPageSourceProvider(ConnectorId connectorId, ConnectorPageSourceProvider pageSourceProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        checkState(pageSourceProviders.put(connectorId, pageSourceProvider) == null, "PageSourceProvider for connector '%s' is already registered", connectorId);
    }

    public void removeConnectorPageSourceProvider(ConnectorId connectorId)
    {
        pageSourceProviders.remove(connectorId);
    }

    @Override
    public ConnectorPageSource createPageSource(Session session, Split split, TableHandle table, List<ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        Optional<Supplier<TupleDomain<ColumnHandle>>> dynamicFilter = table.getDynamicFilter();

        // directly return the result if the given constraint is always false
        if (dynamicFilter.isPresent() && dynamicFilter.get().get().isNone()) {
            return new FixedPageSource(ImmutableList.of());
        }

        if (dynamicFilter.isPresent()) {
            split = new Split(
                    split.getConnectorId(),
                    split.getTransactionHandle(),
                    split.getConnectorSplit(),
                    split.getLifespan(),
                    new SplitContext(split.getSplitContext().isCacheable(), dynamicFilter.get().get()));
        }

        ConnectorSession connectorSession = session.toConnectorSession(split.getConnectorId());
        if (table.getLayout().isPresent()) {
            return getPageSourceProvider(split).createPageSource(
                    split.getTransactionHandle(),
                    connectorSession,
                    split.getConnectorSplit(),
                    table.getLayout().get(),
                    columns,
                    split.getSplitContext());
        }
        return getPageSourceProvider(split).createPageSource(split.getTransactionHandle(), connectorSession, split.getConnectorSplit(), columns, split.getSplitContext());
    }

    private ConnectorPageSourceProvider getPageSourceProvider(Split split)
    {
        ConnectorPageSourceProvider provider = pageSourceProviders.get(split.getConnectorId());

        checkArgument(provider != null, "No page stream provider for '%s", split.getConnectorId());

        return provider;
    }
}
