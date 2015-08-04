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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.facebook.presto.connector.jmx.Types.checkType;
import static com.facebook.presto.spi.TupleDomain.withFixedValues;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class JmxSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NodeManager nodeManager;

    public JmxSplitManager(String connectorId, NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorSession session, ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        requireNonNull(tupleDomain, "tupleDomain is null");
        JmxTableHandle jmxTableHandle = checkType(table, JmxTableHandle.class, "table");

        List<ConnectorPartition> partitions = ImmutableList.of(new JmxPartition(jmxTableHandle, tupleDomain));
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorSession session, ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        requireNonNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(connectorId, ImmutableList.of());
        }

        JmxPartition jmxPartition = checkType(getOnlyElement(partitions), JmxPartition.class, "partition");
        JmxTableHandle tableHandle = jmxPartition.getTableHandle();
        TupleDomain<ColumnHandle> predicate = jmxPartition.getPredicate();

        //TODO is there a better way to get the node column?
        JmxColumnHandle nodeColumnHandle = tableHandle.getColumns().get(0);

        List<ConnectorSplit> splits = nodeManager.getActiveNodes()
                .stream()
                .filter(node -> predicate.overlaps(withFixedValues(ImmutableMap.of(nodeColumnHandle, utf8Slice(node.getNodeIdentifier())))))
                .map(node -> new JmxSplit(tableHandle, ImmutableList.of(node.getHostAndPort())))
                .collect(toList());

        return new FixedSplitSource(connectorId, splits);
    }

    public static class JmxPartition
            implements ConnectorPartition
    {
        private final JmxTableHandle tableHandle;
        private final TupleDomain<ColumnHandle> predicate;

        public JmxPartition(JmxTableHandle tableHandle, TupleDomain<ColumnHandle> predicate)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
        }

        public JmxTableHandle getTableHandle()
        {
            return tableHandle;
        }

        public TupleDomain<ColumnHandle> getPredicate()
        {
            return predicate;
        }

        @Override
        public String getPartitionId()
        {
            return "jmx";
        }

        @Override
        public TupleDomain<ColumnHandle> getTupleDomain()
        {
            return TupleDomain.all();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("tableHandle", tableHandle)
                    .add("predicate", predicate)
                    .toString();
        }
    }
}
