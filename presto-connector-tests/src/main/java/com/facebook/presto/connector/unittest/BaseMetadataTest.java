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
package com.facebook.presto.connector.unittest;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public interface BaseMetadataTest
        extends SPITest
{
    default List<String> systemSchemas()
    {
        return ImmutableList.of();
    }

    default SchemaTableName schemaTableName(String tableName)
    {
        return new SchemaTableName("default_schema", tableName);
    }

    default SchemaTableName schemaTableName(String schemaName, String tableName)
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Test
    default void testEmptyMetadata()
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        run(this,
                ImmutableList.of(
                        metadata -> assertEquals(metadata.listSchemaNames(session), systemSchemas()),
                        metadata -> assertEquals(metadata.listTables(session, null), ImmutableList.of())));
    }

    default List<String> schemaNamesOf(SchemaTableName... schemaTableNames)
    {
        return Arrays.stream(schemaTableNames)
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    default List<String> distinctSchemas(SchemaTableName... schemaTableNames)
    {
        return schemaNamesOf(schemaTableNames).stream()
                .distinct()
                .collect(toImmutableList());
    }

    default SchemaTablePrefix prefixOfSchemaName(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName());
    }

    default SchemaTablePrefix prefixOf(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    default void run(SPITest test, List<Consumer<ConnectorMetadata>> consumers)
    {
        consumers.forEach(consumer -> withMetadata(test, ImmutableList.of(consumer)));
    }

    default void withMetadata(SPITest test, List<Consumer<ConnectorMetadata>> consumers)
    {
        Connector connector = test.getConnector();
        ConnectorTransactionHandle transaction = connector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transaction);
        consumers.forEach(consumer -> consumer.accept(metadata));
        connector.commit(transaction);
    }

    default List<Consumer<ConnectorMetadata>> withSchema(ConnectorSession session, List<String> schemaNames, List<Consumer<ConnectorMetadata>> consumers)
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

    default List<Consumer<ConnectorMetadata>> withTableDropped(ConnectorSession session, List<ConnectorTableMetadata> tables, List<Consumer<ConnectorMetadata>> consumers)
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
}
