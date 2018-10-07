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
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;

import javax.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SplitManager
{
    private final ConcurrentMap<CatalogName, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();
    private final int minScheduleSplitBatchSize;

    @Inject
    public SplitManager(QueryManagerConfig config)
    {
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
    }

    public void addConnectorSplitManager(CatalogName catalogName, ConnectorSplitManager connectorSplitManager)
    {
        requireNonNull(catalogName, "connectorId is null");
        requireNonNull(connectorSplitManager, "connectorSplitManager is null");
        checkState(splitManagers.putIfAbsent(catalogName, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", catalogName);
    }

    public void removeConnectorSplitManager(CatalogName catalogName)
    {
        splitManagers.remove(catalogName);
    }

    public SplitSource getSplits(Session session, TableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        CatalogName catalogName = layout.getCatalogName();
        ConnectorSplitManager splitManager = getConnectorSplitManager(catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        ConnectorSplitSource source = splitManager.getSplits(
                layout.getTransactionHandle(),
                connectorSession,
                layout.getConnectorHandle(),
                splitSchedulingStrategy);

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogName, layout.getTransactionHandle(), source);
        if (minScheduleSplitBatchSize > 1) {
            splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
        }
        return splitSource;
    }

    private ConnectorSplitManager getConnectorSplitManager(CatalogName catalogName)
    {
        ConnectorSplitManager result = splitManagers.get(catalogName);
        checkArgument(result != null, "No split manager for connector '%s'", catalogName);
        return result;
    }
}
