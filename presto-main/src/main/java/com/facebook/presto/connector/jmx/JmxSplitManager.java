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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class JmxSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NodeManager nodeManager;

    @Inject
    public JmxSplitManager(JmxConnectorId jmxConnectorId, NodeManager nodeManager)
    {
        this.connectorId = checkNotNull(jmxConnectorId, "jmxConnectorId is null").toString();
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        checkNotNull(tupleDomain, "tupleDomain is null");
        JmxTableHandle jmxTableHandle = checkType(table, JmxTableHandle.class, "table");

        ImmutableList<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new JmxPartition(jmxTableHandle));
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of());
        }

        ConnectorPartition partition = Iterables.getOnlyElement(partitions);
        JmxTableHandle tableHandle = checkType(partition, JmxPartition.class, "partition").getTableHandle();

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (Node node : nodeManager.getActiveNodes()) {
            splits.add(new JmxSplit(tableHandle, ImmutableList.of(node.getHostAndPort())));
        }
        return new FixedSplitSource(connectorId, splits.build());
    }

    public static class JmxPartition
            implements ConnectorPartition
    {
        private final JmxTableHandle tableHandle;

        public JmxPartition(JmxTableHandle tableHandle)
        {
            this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        }

        public JmxTableHandle getTableHandle()
        {
            return tableHandle;
        }

        @Override
        public String getPartitionId()
        {
            return "jmx";
        }

        @Override
        public TupleDomain<ConnectorColumnHandle> getTupleDomain()
        {
            return TupleDomain.all();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("tableHandle", tableHandle)
                    .toString();
        }
    }
}
