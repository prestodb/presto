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
package com.facebook.presto.plugin.turbonium.systemtables;

import com.facebook.presto.plugin.turbonium.config.TurboniumConfigManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import static com.facebook.presto.spi.SystemTable.Distribution.ALL_NODES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TurboniumConfigSystemTable
    implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final String nodeId;
    private final TurboniumConfigManager configManager;

    @Inject
    public TurboniumConfigSystemTable(TurboniumConfigManager configManager, NodeManager nodeManager)
    {
        this.nodeId = requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getNodeIdentifier();
        this.configManager = requireNonNull(configManager);
        this.tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("system", "config"),
                ImmutableList.of(
                        new ColumnMetadata("node_id", VARCHAR),
                        new ColumnMetadata("max_data_per_node", BIGINT),
                        new ColumnMetadata("max_table_size_per_node", BIGINT),
                        new ColumnMetadata("splits_per_node", BIGINT),
                        new ColumnMetadata("disable_encoding", BOOLEAN)
                )
        );
    }

    @Override
    public SystemTable.Distribution getDistribution()
    {
        return ALL_NODES;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder systemTable = InMemoryRecordSet.builder(tableMetadata);
        systemTable.addRow(nodeId,
                configManager.getConfig().getMaxDataPerNode().toBytes(),
                configManager.getConfig().getMaxTableSizePerNode().toBytes(),
                configManager.getConfig().getSplitsPerNode(),
                configManager.getConfig().getDisableEncoding());
        return systemTable.build().cursor();
    }
}
