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
package com.facebook.presto.connector.dual;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DualSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public DualSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public String getConnectorId()
    {
        // dual is not a connector
        return null;
    }

    @Override
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof DualTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        checkNotNull(table, "table is null");
        checkNotNull(bindings, "bindings is null");

        checkArgument(table instanceof DualTableHandle, "TableHandle must be a DualTableHandle");

        return ImmutableList.<Partition>of(new DualPartition());
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle table, List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        Partition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof DualPartition, "Partition must be a dual partition");

        Optional<Node> currentNode = nodeManager.getCurrentNode();
        Preconditions.checkState(currentNode.isPresent(), "current node is not in the active set");
        ImmutableList<HostAddress> localAddress = ImmutableList.of(currentNode.get().getHostAndPort());

        Split split = new DualSplit(localAddress);

        return ImmutableList.of(split);
    }

    public static class DualPartition
            implements Partition
    {
        @Override
        public String getPartitionId()
        {
            return "dual";
        }

        @Override
        public Map<ColumnHandle, Object> getKeys()
        {
            return ImmutableMap.of();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .toString();
        }
    }
}
