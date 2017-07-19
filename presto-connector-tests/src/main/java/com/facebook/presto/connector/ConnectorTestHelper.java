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
package com.facebook.presto.connector;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.tests.AbstractTestQueryFramework.QueryRunnerSupplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class ConnectorTestHelper
{
    public abstract Connector getConnector()
            throws Exception;

    public abstract QueryRunnerSupplier getQueryRunnerSupplier();

    public abstract Map<String, Object> getTableProperties();

    public List<ColumnMetadata> withInternalColumns(List<ColumnMetadata> expectedColumns)
    {
        return expectedColumns;
    }

    public List<Consumer<ConnectorMetadata>> withSchema(ConnectorSession session, List<String> schemaNames, List<Consumer<ConnectorMetadata>> consumers)
    {
        ImmutableList.Builder<Consumer<ConnectorMetadata>> builder = ImmutableList.builder();

        for (String schemaName : schemaNames) {
            builder.add(metadata -> metadata.createSchema(session, schemaName, ImmutableMap.of()));
        }

        builder.addAll(consumers);

        for (String schemaName : schemaNames) {
            builder.add(metadata -> metadata.dropSchema(session, schemaName));
        }

        return builder.build();
    }

    public List<Consumer<ConnectorMetadata>> withTableDropped(ConnectorSession session, List<ConnectorTableMetadata> tables, List<Consumer<ConnectorMetadata>> consumers)
    {
        ImmutableList.Builder<Consumer<ConnectorMetadata>> builder = ImmutableList.builder();

        List<String> schemaNames = tables.stream()
                .map(ConnectorTableMetadata::getTable)
                .map(SchemaTableName::getSchemaName)
                .distinct()
                .collect(toImmutableList());

        for (ConnectorTableMetadata table : tables) {
            builder.add(metadata -> {
                ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, table, Optional.empty());
                metadata.finishCreateTable(session, handle, ImmutableList.of());
            });
        }

        builder.addAll(consumers);

        for (ConnectorTableMetadata table : tables) {
            builder.add(metadata -> metadata.dropTable(session, metadata.getTableHandle(session, table.getTable())));
        }

        return withSchema(session, schemaNames, builder.build());
    }

    public SchemaTablePrefix prefixOfSchemaName(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName());
    }

    public SchemaTablePrefix prefixOf(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    public List<String> systemSchemas()
    {
        return ImmutableList.of();
    }

    public List<SchemaTableName> systemTables()
    {
        return ImmutableList.of();
    }

    public SchemaTableName schemaTableName(String tableName)
    {
        return new SchemaTableName("default_schema", tableName);
    }

    public SchemaTableName schemaTableName(String schemaName, String tableName)
    {
        return new SchemaTableName(schemaName, tableName);
    }
}
