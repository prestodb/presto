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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.MappedPageSource;
import com.facebook.presto.split.MappedRecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SystemPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Map<SchemaTableName, SystemTable> tables;

    public SystemPageSourceProvider(Set<SystemTable> tables)
    {
        this.tables = uniqueIndex(tables, table -> table.getTableMetadata().getTable());
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        SystemTransactionHandle systemTransaction = checkType(transactionHandle, SystemTransactionHandle.class, "transaction");
        SystemSplit systemSplit = checkType(split, SystemSplit.class, "split");
        SchemaTableName tableName = systemSplit.getTableHandle().getSchemaTableName();
        SystemTable systemTable = tables.get(tableName);

        checkArgument(systemTable != null, "Table %s does not exist", tableName);
        List<ColumnMetadata> tableColumns = systemTable.getTableMetadata().getColumns();

        Map<String, Integer> columnsByName = new HashMap<>();
        for (int i = 0; i < tableColumns.size(); i++) {
            ColumnMetadata column = tableColumns.get(i);
            if (columnsByName.put(column.getName(), i) != null) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Duplicate column name: " + column.getName());
            }
        }

        ImmutableList.Builder<Integer> userToSystemFieldIndex = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            String columnName = checkType(column, SystemColumnHandle.class, "column").getColumnName();

            Integer index = columnsByName.get(columnName);
            if (index == null) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Column does not exist: %s.%s", tableName, columnName));
            }

            userToSystemFieldIndex.add(index);
        }

        TupleDomain<ColumnHandle> constraint = systemSplit.getConstraint();
        ImmutableMap.Builder<Integer, Domain> newConstraints = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
            String columnName = checkType(entry.getKey(), SystemColumnHandle.class, "column").getColumnName();
            newConstraints.put(columnsByName.get(columnName), entry.getValue());
        }
        TupleDomain<Integer> newContraint = withColumnDomains(newConstraints.build());

        try {
            return new MappedPageSource(systemTable.pageSource(systemTransaction.getTransactionHandle(), session, newContraint), userToSystemFieldIndex.build());
        }
        catch (UnsupportedOperationException e) {
            return new RecordPageSource(new MappedRecordSet(toRecordSet(systemTransaction.getTransactionHandle(), systemTable, session, newContraint), userToSystemFieldIndex.build()));
        }
    }

    private static RecordSet toRecordSet(ConnectorTransactionHandle sourceTransaction, SystemTable table, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new RecordSet()
        {
            private final List<Type> types = table.getTableMetadata().getColumns().stream()
                    .map(ColumnMetadata::getType)
                    .collect(toImmutableList());

            @Override
            public List<Type> getColumnTypes()
            {
                return types;
            }

            @Override
            public RecordCursor cursor()
            {
                return table.cursor(sourceTransaction, session, constraint);
            }
        };
    }
}
