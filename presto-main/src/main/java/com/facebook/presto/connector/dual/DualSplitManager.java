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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.util.List;

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
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        checkNotNull(table, "table is null");
        checkNotNull(tupleDomain, "tupleDomain is null");

        checkArgument(table instanceof DualTableHandle, "TableHandle must be a DualTableHandle");

        ImmutableList<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new DualPartition());
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(null, ImmutableList.<ConnectorSplit>of());
        }

        ConnectorPartition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof DualPartition, "Partition must be a dual partition");

        ConnectorSplit split = new DualSplit(nodeManager.getCurrentNode().getHostAndPort());

        return new FixedSplitSource(null, ImmutableList.of(split));
    }

    public static class DualPartition
            implements ConnectorPartition
    {
        @Override
        public String getPartitionId()
        {
            return "dual";
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
                    .toString();
        }
    }
}
