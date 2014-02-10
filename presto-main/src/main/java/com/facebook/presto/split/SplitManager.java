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

import com.facebook.presto.execution.ConnectorAwareSplitSource;
import com.facebook.presto.execution.SplitSource;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Partition;
import com.facebook.presto.metadata.PartitionResult;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.metadata.Partition.connectorPartitionGetter;
import static com.facebook.presto.metadata.Util.toConnectorDomain;

public class SplitManager
{
    private final Set<ConnectorSplitManager> splitManagers = Sets.newSetFromMap(new ConcurrentHashMap<ConnectorSplitManager, Boolean>());

    @Inject
    public SplitManager(Set<ConnectorSplitManager> splitManagers)
    {
        this.splitManagers.addAll(splitManagers);
    }

    public void addConnectorSplitManager(ConnectorSplitManager connectorSplitManager)
    {
        splitManagers.add(connectorSplitManager);
    }

    public PartitionResult getPartitions(TableHandle table, Optional<TupleDomain<ColumnHandle>> tupleDomain)
    {
        ConnectorTableHandle connectorTable = table.getConnectorHandle();
        ConnectorPartitionResult result = getConnectorSplitManager(connectorTable).getPartitions(connectorTable, toConnectorDomain(tupleDomain.or(TupleDomain.<ColumnHandle>all())));

        return new PartitionResult(table.getConnectorId(), result);
    }

    public SplitSource getPartitionSplits(TableHandle handle, List<Partition> partitions)
    {
        ConnectorTableHandle table = handle.getConnectorHandle();
        ConnectorSplitSource source = getConnectorSplitManager(table).getPartitionSplits(table, Lists.transform(partitions, connectorPartitionGetter()));
        return new ConnectorAwareSplitSource(handle.getConnectorId(), source);
    }

    private ConnectorSplitManager getConnectorSplitManager(ConnectorTableHandle handle)
    {
        for (ConnectorSplitManager connectorSplitManager : splitManagers) {
            if (connectorSplitManager.canHandle(handle)) {
                return connectorSplitManager;
            }
        }
        throw new IllegalArgumentException("No split manager for " + handle);
    }
}
