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

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.RecordProjectOperator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.MappedRecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.metadata.MetadataUtil.columnNameGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SystemDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final ConcurrentMap<SchemaTableName, SystemTable> tables = new ConcurrentHashMap<>();

    public void addTable(SystemTable systemTable)
    {
        checkNotNull(systemTable, "systemTable is null");
        SchemaTableName tableName = systemTable.getTableMetadata().getTable();
        checkArgument(tables.putIfAbsent(tableName, systemTable) == null, "Table %s is already registered", tableName);
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof SystemSplit && tables.containsKey(((SystemSplit) split).getTableHandle().getSchemaTableName());
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        return new RecordProjectOperator(operatorContext, createRecordSet(split, columns));
    }

    private RecordSet createRecordSet(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof SystemSplit, "Split must be of type %s, not %s", SystemSplit.class.getName(), split.getClass().getName());
        SchemaTableName tableName = ((SystemSplit) split).getTableHandle().getSchemaTableName();

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        SystemTable systemTable = tables.get(tableName);
        checkArgument(systemTable != null, "Table %s does not exist", tableName);
        Map<String, ColumnMetadata> columnsByName = Maps.uniqueIndex(systemTable.getTableMetadata().getColumns(), columnNameGetter());

        ImmutableList.Builder<Integer> userToSystemFieldIndex = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof SystemColumnHandle, "column must be of type %s, not %s", SystemColumnHandle.class.getName(), column.getClass().getName());
            String columnName = ((SystemColumnHandle) column).getColumnName();

            ColumnMetadata columnMetadata = columnsByName.get(columnName);
            checkArgument(columnMetadata != null, "Column %s.%s does not exist", tableName, columnName);

            userToSystemFieldIndex.add(columnMetadata.getOrdinalPosition());
        }

        return new MappedRecordSet(systemTable, userToSystemFieldIndex.build());
    }
}
