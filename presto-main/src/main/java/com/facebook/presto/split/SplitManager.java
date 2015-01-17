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

import com.facebook.presto.connector.system.SystemSplitManager;
import com.facebook.presto.connector.system.SystemTablesManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Partition;
import com.facebook.presto.metadata.PartitionResult;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.metadata.Util.toConnectorDomain;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class SplitManager
{
    private final ConcurrentMap<String, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();
    private final SystemSplitManager systemSplitManager;

    @Inject
    public SplitManager(SystemSplitManager systemSplitManager)
    {
        this.systemSplitManager = checkNotNull(systemSplitManager, "systemSplitManager is null");
    }

    public void addConnectorSplitManager(String connectorId, ConnectorSplitManager connectorSplitManager)
    {
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public PartitionResult getPartitions(TableHandle table, Optional<TupleDomain<ColumnHandle>> tupleDomain)
    {
        TupleDomain<ColumnHandle> domain = tupleDomain.orElse(TupleDomain.all());

        ConnectorPartitionResult result;
        if (domain.isNone()) {
            result = new ConnectorPartitionResult(ImmutableList.<ConnectorPartition>of(), toConnectorDomain(domain));
        }
        else {
            result = getConnectorSplitManager(table).getPartitions(table.getConnectorHandle(), toConnectorDomain(domain));
        }

        return new PartitionResult(table.getConnectorId(), result);
    }

    public SplitSource getPartitionSplits(TableHandle handle, List<Partition> partitions)
    {
        if (partitions.isEmpty()) {
            return new ConnectorAwareSplitSource(handle.getConnectorId(), new FixedSplitSource(handle.getConnectorId(), ImmutableList.<ConnectorSplit>of()));
        }
        ConnectorTableHandle table = handle.getConnectorHandle();
        ConnectorSplitSource source = getConnectorSplitManager(handle).getPartitionSplits(table, Lists.transform(partitions, Partition::getConnectorPartition));
        return new ConnectorAwareSplitSource(handle.getConnectorId(), source);
    }

    private ConnectorSplitManager getConnectorSplitManager(TableHandle handle)
    {
        String connectorId = handle.getConnectorId();

        if (connectorId.equals(SystemTablesManager.CONNECTOR_ID)) {
            return systemSplitManager;
        }

        ConnectorSplitManager result = splitManagers.get(connectorId);
        checkArgument(result != null, "No split manager for connector '%s'", connectorId);

        return result;
    }
}
