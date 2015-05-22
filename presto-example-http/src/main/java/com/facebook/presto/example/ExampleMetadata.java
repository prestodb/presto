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
package com.facebook.presto.example;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.example.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExampleMetadata
        extends ReadOnlyConnectorMetadata
{
    private final String connectorId;

    private final ExampleClient exampleClient;

    @Inject
    public ExampleMetadata(ExampleConnectorId connectorId, ExampleClient exampleClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.exampleClient = checkNotNull(exampleClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(exampleClient.getSchemaNames());
    }

    @Override
    public ExampleTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        ExampleTable table = exampleClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ExampleTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table)
    {
        ExampleTableHandle exampleTableHandle = checkType(table, ExampleTableHandle.class, "table");
        checkArgument(exampleTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = exampleClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : exampleClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        ExampleTableHandle exampleTableHandle = checkType(tableHandle, ExampleTableHandle.class, "tableHandle");
        checkArgument(exampleTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        ExampleTable table = exampleClient.getTable(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(exampleTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new ExampleColumnHandle(connectorId, column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        ExampleTable table = exampleClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, ExampleTableHandle.class, "tableHandle");
        return checkType(columnHandle, ExampleColumnHandle.class, "columnHandle").getColumnMetadata();
    }
}
