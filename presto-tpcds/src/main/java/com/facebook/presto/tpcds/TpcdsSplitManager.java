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
package com.facebook.presto.tpcds;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.Set;

import static com.facebook.presto.tpcds.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TpcdsSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;
    private final boolean noSexism;

    public TpcdsSplitManager(NodeManager nodeManager, int splitsPerNode, boolean noSexism)
    {
        requireNonNull(nodeManager);
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");

        this.nodeManager = nodeManager;
        this.splitsPerNode = splitsPerNode;
        this.noSexism = noSexism;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        TpcdsTableHandle tableHandle = checkType(layout, TpcdsTableLayoutHandle.class, "layout").getTable();

        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No TPCDS nodes available");

        int totalParts = nodes.size() * splitsPerNode;
        int partNumber = 0;

        // Split the data using split and skew by the number of nodes available.
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (Node node : nodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new TpcdsSplit(tableHandle, partNumber, totalParts, ImmutableList.of(node.getHostAndPort()), noSexism));
                partNumber++;
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
