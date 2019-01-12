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
package io.prestosql.plugin.localfile;

import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

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
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        LocalFileTableLayoutHandle layoutHandle = (LocalFileTableLayoutHandle) layout;
        LocalFileTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<LocalFileColumnHandle> effectivePredicate = layoutHandle.getConstraint()
                .transform(LocalFileColumnHandle.class::cast);

        List<ConnectorSplit> splits = nodeManager.getAllNodes().stream()
                .map(node -> new LocalFileSplit(node.getHostAndPort(), tableHandle.getSchemaTableName(), effectivePredicate))
                .collect(Collectors.toList());

        return new FixedSplitSource(splits);
    }
}
