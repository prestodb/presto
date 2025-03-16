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

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class TpcdsSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;
    private final boolean noSexism;
    private final boolean nativeExecution;

    public TpcdsSplitManager(NodeManager nodeManager, int splitsPerNode, boolean noSexism, boolean nativeExecution)
    {
        requireNonNull(nodeManager);
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");

        this.nodeManager = nodeManager;
        this.splitsPerNode = splitsPerNode;
        this.noSexism = noSexism;
        this.nativeExecution = nativeExecution;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        TpcdsTableHandle tableHandle = ((TpcdsTableLayoutHandle) layout).getTable();

        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No TPCDS nodes available");

        int totalParts = nodes.size() * splitsPerNode;
        int partNumber = 0;

        // For larger tables, split the data using split and skew by the number of nodes available.
        // The TPCDS connector in presto native uses dsdgen-c for data generation. For certain smaller tables,
        // the data cannot be generated in parallel. For these cases, a single split should be processed by
        // only one of the worker nodes.
        Set<String> smallTables = unmodifiableSet(new HashSet<>(asList("call_center", "item", "store", "web_page", "web_site")));
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        if (nativeExecution && smallTables.contains(tableHandle.getTableName())) {
            Node node = nodes.stream()
                    .findFirst()
                    .orElse(null);
            splits.add(new TpcdsSplit(tableHandle, 0, 1, ImmutableList.of(node.getHostAndPort()), noSexism));
        }
        else {
            for (Node node : nodes) {
                for (int i = 0; i < splitsPerNode; i++) {
                    splits.add(new TpcdsSplit(tableHandle, partNumber, totalParts, ImmutableList.of(node.getHostAndPort()), noSexism));
                    partNumber++;
                }
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
