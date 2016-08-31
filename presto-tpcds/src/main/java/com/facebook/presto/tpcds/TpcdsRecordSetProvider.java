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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;

import java.util.List;

import static com.facebook.presto.tpcds.Types.checkType;
import static com.teradata.tpcds.Results.constructResults;
import static com.teradata.tpcds.Table.getTable;

public class TpcdsRecordSetProvider
        implements ConnectorRecordSetProvider
{
    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        TpcdsSplit tpcdsSplit = checkType(split, TpcdsSplit.class, "split");
        String tableName = tpcdsSplit.getTableHandle().getTableName();
        Table table = getTable(tableName);
        return getRecordSet(table, columns, tpcdsSplit.getTableHandle().getScaleFactor(), tpcdsSplit.getPartNumber(), tpcdsSplit.getTotalParts(), tpcdsSplit.isNoSexism());
    }

    private RecordSet getRecordSet(
            Table table,
            List<? extends ColumnHandle> columns,
            int scaleFactor,
            int partNumber,
            int totalParts,
            boolean noSexism)
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            String columnName = checkType(column, TpcdsColumnHandle.class, "column").getColumnName();
            builder.add(table.getColumn(columnName));
        }

        Session session = Session.getDefaultSession()
                .withScale(scaleFactor)
                .withParallelism(totalParts)
                .withChunkNumber(partNumber + 1)
                .withTable(table)
                .withNoSexism(noSexism);
        Results results = constructResults(table, session);
        return new TpcdsRecordSet(results, builder.build());
    }
}
