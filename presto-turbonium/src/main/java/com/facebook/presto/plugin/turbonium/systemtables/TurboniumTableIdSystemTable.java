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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TurboniumTableIdSystemTable
        implements SystemTable
{
    private final TurboniumMetadata metadata;
    private final ConnectorTableMetadata tableMetadata;

    @Inject
    public TurboniumTableIdSystemTable(TurboniumMetadata metadata)
    {
        this.metadata = metadata;
        this.tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("system", "table_ids"),
                ImmutableList.of(
                        new ColumnMetadata("table_name", VARCHAR),
                        new ColumnMetadata("table_id", BIGINT)));
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
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
        for (Map.Entry<String, Long> table : metadata.getTableIds().entrySet()) {
            systemTable.addRow(table.getKey(), table.getValue());
        }
        return systemTable.build().cursor();
    }
}
