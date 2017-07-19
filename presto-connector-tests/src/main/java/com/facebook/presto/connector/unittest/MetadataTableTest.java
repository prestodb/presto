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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public interface MetadataTableTest
        extends BaseMetadataTest
{
    Map<String, Object> getTableProperties();

    List<ColumnMetadata> getConnectorColumns();

    @Test
    default void testCreateDropTable()
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

        run(this,
                withSchema(session, schemaNamesOf(schemaTableName),
                        ImmutableList.of(
                                metadata -> metadata.createTable(session, tableMetadata),
                                metadata -> assertEquals(getOnlyElement(metadata.listTables(session, schemaTableName.getSchemaName())), schemaTableName),
                                metadata -> metadata.dropTable(session, metadata.getTableHandle(session, schemaTableName)))));
    }

    default void testRenameTable(SchemaTableName initial, SchemaTableName renamed)
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                initial,
                ImmutableList.of(
                        new ColumnMetadata("bigint_column", BIGINT),
                        new ColumnMetadata("double_column", DOUBLE)),
                getTableProperties());

        run(this,
                withSchema(session, distinctSchemas(initial, renamed),
                        ImmutableList.of(
                                metadata -> {
                                    ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
                                    metadata.finishCreateTable(session, handle, ImmutableList.of());
                                },
                                metadata -> metadata.renameTable(session, metadata.getTableHandle(session, initial), renamed),
                                metadata -> assertEquals(getOnlyElement(metadata.listTables(session, renamed.getSchemaName())), renamed),
                                metadata -> metadata.dropTable(session, metadata.getTableHandle(session, renamed)))));
    }

    @Test
    default void testRenameTableWithinSchema()
    {
        testRenameTable(
                schemaTableName("initial"),
                schemaTableName("renamed"));
    }

    @Test
    default void testRenameTableAcrossSchema()
    {
        testRenameTable(
                schemaTableName("initialSchema", "initialTable"),
                schemaTableName("renamedSchema", "renamedTable"));
    }

    @Test
    default void testListColumnsOneSchema()
    {
        SchemaTableName schemaTableName1 = schemaTableName("table1");
        SchemaTableName schemaTableName2 = schemaTableName("table2");

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        List<ColumnMetadata> columns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        List<ColumnMetadata> expectedColumns = ImmutableList.<ColumnMetadata>builder()
                .addAll(columns)
                .addAll(getConnectorColumns())
                .build();

        ConnectorTableMetadata tableMetadata11 = new ConnectorTableMetadata(schemaTableName1, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata12 = new ConnectorTableMetadata(schemaTableName2, columns, getTableProperties());

        run(this,
                withTableDropped(session, ImmutableList.of(tableMetadata11, tableMetadata12),
                        ImmutableList.of(
                                metadata -> assertEquals(metadata.listTableColumns(session, new SchemaTablePrefix()),
                                        ImmutableMap.of(
                                                schemaTableName1, expectedColumns,
                                                schemaTableName2, expectedColumns)),
                                metadata -> assertEquals(metadata.listTableColumns(session, prefixOf(schemaTableName1)),
                                        ImmutableMap.of(
                                                schemaTableName1, expectedColumns)))));
    }

    @Test
    default void testListColumnsMultiSchema()
    {
        String schemaName1 = "testListColumns1";
        String schemaName2 = "testListColumns2";
        SchemaTableName schemaTableName11 = schemaTableName(schemaName1, "table1");
        SchemaTableName schemaTableName12 = schemaTableName(schemaName1, "table2");
        SchemaTableName schemaTableName2 = schemaTableName(schemaName2, "table");

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        List<ColumnMetadata> columns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        List<ColumnMetadata> expectedColumns = ImmutableList.<ColumnMetadata>builder()
                .addAll(columns)
                .addAll(getConnectorColumns())
                .build();

        ConnectorTableMetadata tableMetadata11 = new ConnectorTableMetadata(schemaTableName11, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata12 = new ConnectorTableMetadata(schemaTableName12, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata2 = new ConnectorTableMetadata(schemaTableName2, columns, getTableProperties());

        run(this,
                withTableDropped(session, ImmutableList.of(tableMetadata11, tableMetadata12, tableMetadata2),
                        ImmutableList.of(
                                metadata -> assertEquals(metadata.listTableColumns(session, new SchemaTablePrefix()),
                                        ImmutableMap.of(
                                                schemaTableName11, expectedColumns,
                                                schemaTableName12, expectedColumns,
                                                schemaTableName2, expectedColumns)),
                                metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName2)),
                                        ImmutableMap.of(
                                                schemaTableName2, expectedColumns)))));
    }

    @Test
    default void testAddColumn()
    {
        SchemaTableName schemaTableName = schemaTableName("table");
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        List<ColumnMetadata> initialColumns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        ColumnMetadata newColumn = new ColumnMetadata("varchar_column", VARCHAR);

        List<ColumnMetadata> expectedInitialColumns = ImmutableList.<ColumnMetadata>builder()
                .addAll(initialColumns)
                .addAll(getConnectorColumns())
                .build();

        List<ColumnMetadata> expectedFinalColumns = ImmutableList.<ColumnMetadata>builder()
                .addAll(initialColumns)
                .add(newColumn)
                .addAll(getConnectorColumns())
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, initialColumns, getTableProperties());

        run(this,
                withTableDropped(session, ImmutableList.of(tableMetadata),
                        ImmutableList.of(
                                metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName)),
                                        ImmutableMap.of(schemaTableName, expectedInitialColumns)),
                                metadata -> metadata.addColumn(session, metadata.getTableHandle(session, schemaTableName), newColumn),
                                metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName)),
                                        ImmutableMap.of(schemaTableName, expectedFinalColumns)))));
    }

    @Test
    default void testRenameColumn()
    {
        SchemaTableName schemaTableName = schemaTableName("table");
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        ColumnMetadata unchanging = new ColumnMetadata("bigint_column", BIGINT);
        ColumnMetadata initial = new ColumnMetadata("initial_column", DOUBLE);
        ColumnMetadata renamed = new ColumnMetadata("renamed_column", DOUBLE);

        List<ColumnMetadata> initialColumns = ImmutableList.of(unchanging, initial);

        List<ColumnMetadata> expectedInitialColumns = ImmutableList.<ColumnMetadata>builder()
                .addAll(initialColumns)
                .addAll(getConnectorColumns())
                .build();

        List<ColumnMetadata> expectedFinalColumns = ImmutableList.<ColumnMetadata>builder()
                .add(unchanging)
                .add(renamed)
                .addAll(getConnectorColumns())
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, initialColumns, getTableProperties());

        run(this,
                withTableDropped(session, ImmutableList.of(tableMetadata),
                        ImmutableList.of(
                                metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName)),
                                        ImmutableMap.of(schemaTableName, expectedInitialColumns)),
                                metadata -> {
                                    ConnectorTableHandle table = metadata.getTableHandle(session, schemaTableName);
                                    Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, table);
                                    metadata.renameColumn(
                                            session,
                                            table,
                                            columns.get("initial_column"),
                                            "renamed_column");
                                },
                                metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName)),
                                        ImmutableMap.of(schemaTableName, expectedFinalColumns)))));
    }
}
