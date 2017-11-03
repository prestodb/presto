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

import com.facebook.presto.plugin.turbonium.TurboniumMetadata;
import com.facebook.presto.plugin.turbonium.TurboniumPagesStore;
import com.facebook.presto.plugin.turbonium.TurboniumPagesStore.ColumnSizeInfo;
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
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TurboniumColumnStatsSystemTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final TurboniumPagesStore pagesStore;
    private final String nodeId;

    @Inject
    public TurboniumColumnStatsSystemTable(TurboniumPagesStore pagesStore, NodeManager nodeManager)
    {
        this.nodeId = requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getNodeIdentifier();
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("system", "column_stats"),
                ImmutableList.of(
                        new ColumnMetadata("node_id", VARCHAR),
                        new ColumnMetadata("schema_name", VARCHAR),
                        new ColumnMetadata("table_id", BIGINT),
                        new ColumnMetadata("column_id", BIGINT),
                        new ColumnMetadata("size_bytes", BIGINT),
                        new ColumnMetadata("source_size_bytes", BIGINT)));
    }

    @Override
    public Distribution getDistribution()
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
        for (long tableId : pagesStore.listTableIds()) {
            int columnId = 0;
            for (ColumnSizeInfo columnSizeInfo : pagesStore.getColumnSizes(tableId)) {
                systemTable.addRow(
                        nodeId,
                        TurboniumMetadata.SCHEMA_NAME,
                        tableId,
                        columnId++,
                        columnSizeInfo.getSizeBytes(),
                        columnSizeInfo.getSourceSize());
            }
        }
        return systemTable.build().cursor();
    }
}
