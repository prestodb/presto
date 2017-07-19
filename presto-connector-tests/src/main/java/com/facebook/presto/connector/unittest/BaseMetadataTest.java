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

import com.facebook.presto.connector.meta.RequiredFeatures;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
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
import com.google.common.collect.Streams;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.jupiter.api.Assertions.assertEquals;

public interface BaseMetadataTest
        extends SPITest
{
    Map<String, Object> getTableProperties();

    default List<ColumnMetadata> withSystemColumns(List<ColumnMetadata> expectedColumns)
    {
        return expectedColumns;
    }

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

        run(ImmutableList.of(
                metadata -> assertEquals(metadata.listSchemaNames(session), systemSchemas()),
                metadata -> assertEquals(metadata.listTables(session, null), ImmutableList.of())));
    }

    @Test
    @RequiredFeatures({CREATE_TABLE, DROP_TABLE})
    default void testCreateDropTable()
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        String tableName = "table";
        SchemaTableName schemaTableName = schemaTableName(tableName);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                schemaTableName,
                ImmutableList.of(
                        new ColumnMetadata("bigint_column", BIGINT),
                        new ColumnMetadata("double_column", DOUBLE)),
                getTableProperties());

        try (AllCloser cleanup = AllCloser.of(new Schema(this, session, schemaTableName.getSchemaName()))) {
            run(ImmutableList.of(
                    metadata -> metadata.createTable(session, tableMetadata),
                    metadata -> assertEquals(getOnlyElement(metadata.listTables(session, schemaTableName.getSchemaName())), schemaTableName),
                    metadata -> metadata.dropTable(session, metadata.getTableHandle(session, schemaTableName))));
        }
    }

    default List<String> schemaNamesOf(SchemaTableName... schemaTableNames)
    {
        return Arrays.stream(schemaTableNames)
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    default List<String> schemaNamesOf(List<ConnectorTableMetadata> tables)
    {
        return tables.stream()
                .map(ConnectorTableMetadata::getTable)
                .map(SchemaTableName::getSchemaName)
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

    default void run(List<Consumer<ConnectorMetadata>> consumers)
    {
        consumers.forEach(this::withMetadata);
    }

    default void withMetadata(Consumer<ConnectorMetadata> consumer)
    {
        Connector connector = getConnector();
        ConnectorTransactionHandle transaction = connector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transaction);
        consumer.accept(metadata);
        connector.commit(transaction);
    }

    class AllCloser
            implements AutoCloseable
    {
        List<AutoCloseable> items;

        public AllCloser(List<AutoCloseable> items)
        {
            this.items = items;
        }

        static AllCloser of(AutoCloseable... elements)
        {
            return new AllCloser(ImmutableList.copyOf(elements));
        }

        @Override
        public void close()
                throws Exception
        {
            for (int i = items.size() - 1; i >= 0; --i) {
                items.get(i).close();
            }
        }
    }

    class Schema
            implements AutoCloseable
    {
        private final BaseMetadataTest test;
        private final ConnectorSession session;
        private final String name;

        public Schema(BaseMetadataTest test, ConnectorSession session, String name)
        {
            this.test = test;
            this.session = session;
            this.name = name;

            test.withMetadata(metadata -> metadata.createSchema(session, name, ImmutableMap.of()));
        }

        @Override
        public void close()
                throws Exception
        {
            test.withMetadata(metadata -> metadata.dropSchema(session, name));
        }
    }

    default List<AutoCloseable> withSchemas(ConnectorSession session, List<String> schemaNames)
    {
        return schemaNames.stream()
                .distinct()
                .map(schemaName -> new Schema(this, session, schemaName))
                .collect(toImmutableList());
    }

    class Table
            implements AutoCloseable
    {
        private final BaseMetadataTest test;
        private final ConnectorSession session;
        private final ConnectorTableMetadata tableMetadata;

        public Table(BaseMetadataTest test, ConnectorSession session, ConnectorTableMetadata tableMetadata)
        {
            this.test = test;
            this.session = session;
            this.tableMetadata = tableMetadata;

            test.withMetadata(metadata -> {
                ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
                metadata.finishCreateTable(session, handle, ImmutableList.of());
            });
        }

        @Override
        public void close()
                throws Exception
        {
            test.withMetadata(metadata -> {
                ConnectorTableHandle handle = metadata.getTableHandle(session, tableMetadata.getTable());
                metadata.dropTable(session, handle);
            });
        }
    }

    default AllCloser withTables(ConnectorSession session, ConnectorTableMetadata... tables)
    {
        List<ConnectorTableMetadata> tableList = Arrays.asList(tables);
        return new AllCloser(Streams.concat(
                withSchemas(session, schemaNamesOf(tableList)).stream(),
                tableList.stream()
                        .map(table -> new Table(this, session, table)))
                .collect(toImmutableList()));
    }
}
