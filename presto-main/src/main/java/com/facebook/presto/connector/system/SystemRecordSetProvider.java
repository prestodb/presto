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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.split.MappedRecordSet;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.uniqueIndex;

public class SystemRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final Map<SchemaTableName, SystemTable> tables;

    public SystemRecordSetProvider(Set<SystemTable> tables)
    {
        this.tables = uniqueIndex(tables, table -> table.getTableMetadata().getTable());
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        SchemaTableName tableName = checkType(split, SystemSplit.class, "split").getTableHandle().getSchemaTableName();

        checkNotNull(columns, "columns is null");

        SystemTable systemTable = tables.get(tableName);
        checkArgument(systemTable != null, "Table %s does not exist", tableName);
        Map<String, ColumnMetadata> columnsByName = uniqueIndex(systemTable.getTableMetadata().getColumns(), ColumnMetadata::getName);

        ImmutableList.Builder<Integer> userToSystemFieldIndex = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            String columnName = checkType(column, SystemColumnHandle.class, "column").getColumnName();

            ColumnMetadata columnMetadata = columnsByName.get(columnName);
            checkArgument(columnMetadata != null, "Column %s.%s does not exist", tableName, columnName);

            userToSystemFieldIndex.add(columnMetadata.getOrdinalPosition());
        }

        return new MappedRecordSet(systemTable, userToSystemFieldIndex.build());
    }
}
