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
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.TableFunctionHandle;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingContext;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import jakarta.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.LEGACY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SplitManager
{
    private final ConcurrentMap<ConnectorId, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();
    private final int minScheduleSplitBatchSize;
    private final Metadata metadata;
    private final boolean preferSplitHostAddresses;

    @Inject
    public SplitManager(MetadataManager metadata, QueryManagerConfig config, NodeSchedulerConfig nodeSchedulerConfig)
    {
        this.metadata = metadata;
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
        this.preferSplitHostAddresses = !nodeSchedulerConfig.getNetworkTopology().equals(LEGACY);
    }

    public void addConnectorSplitManager(ConnectorId connectorId, ConnectorSplitManager connectorSplitManager)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(connectorSplitManager, "connectorSplitManager is null");
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public void removeConnectorSplitManager(ConnectorId connectorId)
    {
        splitManagers.remove(connectorId);
    }

    public SplitSource getSplits(Session session, TableHandle table, SplitSchedulingStrategy splitSchedulingStrategy, WarningCollector warningCollector)
    {
        long startTime = System.nanoTime();
        ConnectorId connectorId = table.getConnectorId();
        ConnectorSplitManager splitManager = getConnectorSplitManager(connectorId);

        ConnectorSession connectorSession = session.toConnectorSession(connectorId);
        // Now we will fetch the layout handle if it's not presented in TableHandle.
        // In the future, ConnectorTableHandle will be used to fetch splits since it will contain layout information.
        ConnectorTableLayoutHandle layout;
        if (!table.getLayout().isPresent()) {
            TableLayoutResult result = metadata.getLayout(session, table, Constraint.alwaysTrue(), Optional.empty());
            layout = result.getLayout().getLayoutHandle();
        }
        else {
            layout = table.getLayout().get();
        }

        ConnectorSplitSource source = splitManager.getSplits(
                table.getTransaction(),
                connectorSession,
                layout,
                new SplitSchedulingContext(splitSchedulingStrategy, preferSplitHostAddresses, warningCollector));

        SplitSource splitSource = new ConnectorAwareSplitSource(connectorId, table.getTransaction(), source);
        if (minScheduleSplitBatchSize > 1) {
            splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
        }
        return splitSource;
    }

    private ConnectorSplitManager getConnectorSplitManager(ConnectorId connectorId)
    {
        ConnectorSplitManager result = splitManagers.get(connectorId);
        checkArgument(result != null, "No split manager for connector '%s'", connectorId);
        return result;
    }

    public SplitSource getSplitsForTableFunction(Session session, TableFunctionHandle function)
    {
        ConnectorId connectorId = function.getConnectorId();
        ConnectorSplitManager splitManager = splitManagers.get(connectorId);

        ConnectorSplitSource source = splitManager.getSplits(
                function.getTransactionHandle(),
                session.toConnectorSession(connectorId),
                function.getFunctionHandle());

        return new ConnectorAwareSplitSource(connectorId, function.getTransactionHandle(), source);
    }
}
