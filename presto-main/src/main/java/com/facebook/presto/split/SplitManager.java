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

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    public PartitionResult getPartitions(TableHandle table, Optional<TupleDomain> tupleDomain)
    {
        return getConnectorSplitManager(table).getPartitions(table, tupleDomain.or(TupleDomain.all()));
    }

    public DataSource getPartitionSplits(TableHandle handle, List<Partition> partitions)
    {
        ConnectorSplitManager connectorSplitManager = getConnectorSplitManager(handle);
        String connectorId = connectorSplitManager.getConnectorId();
        return new DataSource(connectorId, connectorSplitManager.getPartitionSplits(handle, partitions));
    }

    private ConnectorSplitManager getConnectorSplitManager(TableHandle handle)
    {
        for (ConnectorSplitManager connectorSplitManager : splitManagers) {
            if (connectorSplitManager.canHandle(handle)) {
                return connectorSplitManager;
            }
        }
        throw new IllegalArgumentException("No split manager for " + handle);
    }
}
