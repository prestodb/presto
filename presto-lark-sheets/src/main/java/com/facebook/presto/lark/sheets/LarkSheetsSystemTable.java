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

package com.facebook.presto.lark.sheets;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.lark.sheets.api.SheetInfo;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public class LarkSheetsSystemTable
        implements SystemTable
{
    private static final String IDENTIFIER = "$sheets";
    private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
            new ColumnMetadata("index", INTEGER),
            new ColumnMetadata("sheetId", VARCHAR),
            new ColumnMetadata("title", VARCHAR));

    private final ConnectorTableMetadata metadata;
    private final List<SheetInfo> sheets;

    public static boolean requestsSheets(String tableName)
    {
        return IDENTIFIER.equalsIgnoreCase(tableName);
    }

    public LarkSheetsSystemTable(String schemaName, List<SheetInfo> sheets)
    {
        requireNonNull(schemaName, "schemaName is null");
        this.sheets = requireNonNull(sheets, "sheets is null");
        this.metadata = new ConnectorTableMetadata(new SchemaTableName(schemaName, IDENTIFIER), COLUMNS);
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return metadata;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(metadata);
        sheets.stream()
                .sorted(SheetInfo.indexComparator())
                .forEach(sheet -> {
                    table.addRow(sheet.getIndex(), sheet.getSheetId(), sheet.getTitle());
                });
        return table.build().cursor();
    }
}
