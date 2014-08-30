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
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.metadata.Partition.connectorPartitionGetter;
import static com.facebook.presto.metadata.Util.toConnectorDomain;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class SplitManager
{
    private final ConcurrentMap<String, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();

    public void addConnectorSplitManager(String connectorId, ConnectorSplitManager connectorSplitManager)
    {
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public PartitionResult getPartitions(TableHandle table, Optional<TupleDomain<ColumnHandle>> tupleDomain)
    {
        TupleDomain<ColumnHandle> domain = tupleDomain.or(TupleDomain.<ColumnHandle>all());

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
        ConnectorTableHandle table = handle.getConnectorHandle();
        ConnectorSplitSource source = getConnectorSplitManager(handle).getPartitionSplits(table, Lists.transform(partitions, connectorPartitionGetter()));
        return new ConnectorAwareSplitSource(handle.getConnectorId(), source);
    }

    private ConnectorSplitManager getConnectorSplitManager(TableHandle handle)
    {
        ConnectorSplitManager result = splitManagers.get(handle.getConnectorId());

        checkArgument(result != null, "No split manager for connector '%s'", handle.getConnectorId());

        return result;
    }
}
