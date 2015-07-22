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
package com.facebook.presto.connector.system;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.uniqueIndex;

public class SystemSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final Map<SchemaTableName, SystemTable> tables;

    public SystemSplitManager(NodeManager nodeManager, Set<SystemTable> tables)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.tables = uniqueIndex(tables, table -> table.getTableMetadata().getTable());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTableLayoutHandle layout)
    {
        SystemTableLayoutHandle layoutHandle = checkType(layout, SystemTableLayoutHandle.class, "layout");
        SystemTableHandle tableHandle = layoutHandle.getTable();
        SystemTable systemTable = tables.get(tableHandle.getSchemaTableName());

        if (systemTable.isDistributed()) {
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (Node node : nodeManager.getActiveNodes()) {
                splits.add(new SystemSplit(tableHandle, node.getHostAndPort()));
            }
            return new FixedSplitSource(SystemConnector.NAME, splits.build());
        }

        HostAddress address = nodeManager.getCurrentNode().getHostAndPort();
        ConnectorSplit split = new SystemSplit(tableHandle, address);
        return new FixedSplitSource(SystemConnector.NAME, ImmutableList.of(split));
    }
}
