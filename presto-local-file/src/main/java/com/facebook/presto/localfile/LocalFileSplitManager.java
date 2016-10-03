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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.localfile.Types.checkType;
import static java.util.Objects.requireNonNull;

public class LocalFileSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public LocalFileSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        LocalFileTableLayoutHandle layoutHandle = checkType(layout, LocalFileTableLayoutHandle.class, "layout");
        LocalFileTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<LocalFileColumnHandle> effectivePredicate = layoutHandle.getConstraint()
                .transform(handle -> checkType(handle, LocalFileColumnHandle.class, "columnHandle"));

        List<ConnectorSplit> splits = nodeManager.getAllNodes().stream()
                .map(node -> new LocalFileSplit(node.getHostAndPort(), tableHandle.getSchemaTableName(), effectivePredicate))
                .collect(Collectors.toList());

        return new FixedSplitSource(splits);
    }
}
