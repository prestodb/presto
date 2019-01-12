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
package io.prestosql.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class TpchSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public TpchSplitManager(NodeManager nodeManager, int splitsPerNode)
    {
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        TpchTableLayoutHandle tableLayoutHandle = (TpchTableLayoutHandle) layout;
        TpchTableHandle tableHandle = tableLayoutHandle.getTable();

        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();

        int totalParts = nodes.size() * splitsPerNode;
        int partNumber = 0;

        // Split the data using split and skew by the number of nodes available.
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (Node node : nodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new TpchSplit(tableHandle, partNumber, totalParts, ImmutableList.of(node.getHostAndPort()), tableLayoutHandle.getPredicate()));
                partNumber++;
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
