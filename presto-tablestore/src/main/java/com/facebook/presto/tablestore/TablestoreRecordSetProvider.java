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
package com.facebook.presto.tablestore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TablestoreRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(TablestoreRecordSetProvider.class);
    private final TablestoreFacade tablestoreFacade;
    private static final int MAX_PRINT_SIZE = 5;

    @Inject
    public TablestoreRecordSetProvider(TablestoreFacade tablestoreFacade)
    {
        this.tablestoreFacade = requireNonNull(tablestoreFacade, "tablestoreFacade is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorSplit split,
            List<? extends ColumnHandle> columns)
    {
        TablestoreSplit tablestoreSplit = TypeUtil.checkType(split, TablestoreSplit.class);

        String columnStr = printColumnsToGet(columns, MAX_PRINT_SIZE);
        String stn = tablestoreSplit.getTableHandle().getPrintableStn();
        log.info("queryId=%s getRecordSet() of table:%s and columns:%s", session.getQueryId(), stn, columnStr);

        List<TablestoreColumnHandle> handles = columns.stream()
                .map(c -> TypeUtil.checkType(c, TablestoreColumnHandle.class))
                .collect(Collectors.toList());

        return new TablestoreRecordSet(tablestoreFacade, session, tablestoreSplit, handles);
    }

    protected static String printColumnsToGet(List<? extends ColumnHandle> handles, int maxPrintSize)
    {
        StringBuilder columnStr = new StringBuilder();
        if (handles.size() == 0) {
            return "<none columns>";
        }
        int length = Math.min(maxPrintSize, handles.size());
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                columnStr.append(", ");
            }
            TablestoreColumnHandle c = ((TablestoreColumnHandle) handles.get(i));
            columnStr.append(c.getColumnName());
        }
        if (handles.size() > maxPrintSize) {
            columnStr.append(", ..., <other ").append(handles.size() - maxPrintSize).append(" columns>");
        }
        return columnStr.toString();
    }
}
