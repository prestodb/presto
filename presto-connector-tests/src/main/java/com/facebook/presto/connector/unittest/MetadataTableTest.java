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

import static com.facebook.presto.connector.meta.ConnectorFeature.ADD_COLUMN;
import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE_AS;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.RENAME_COLUMN;
import static com.facebook.presto.connector.meta.ConnectorFeature.RENAME_TABLE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@RequiredFeatures({CREATE_TABLE_AS, DROP_TABLE})
public interface MetadataTableTest
        extends BaseMetadataTest
{
    default void testRenameTable(SchemaTableName initial, SchemaTableName renamed)
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                initial,
                ImmutableList.of(
                        new ColumnMetadata("bigint_column", BIGINT),
                        new ColumnMetadata("double_column", DOUBLE)),
                getTableProperties());

        withSchemas(session, schemaNamesOf(initial, renamed),
                ImmutableList.of(
                        metadata -> {
                            ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
                            metadata.finishCreateTable(session, handle, ImmutableList.of());
                        },
                        metadata -> metadata.renameTable(session, metadata.getTableHandle(session, initial), renamed),
                        metadata -> assertEquals(getOnlyElement(metadata.listTables(session, renamed.getSchemaName())), renamed),
                        metadata -> metadata.dropTable(session, metadata.getTableHandle(session, renamed))));
    }

    @Test
    @RequiredFeatures(RENAME_TABLE)
    default void testRenameTableWithinSchema()
            throws Exception
    {
        testRenameTable(
                tableInDefaultSchema("initial"),
                tableInDefaultSchema("renamed"));
    }

    @Test
    @RequiredFeatures({CREATE_SCHEMA, DROP_SCHEMA, RENAME_TABLE})
    default void testRenameTableAcrossSchema()
            throws Exception
    {
        testRenameTable(
                tableInSpecificSchema("initialSchema", "initialTable"),
                tableInSpecificSchema("renamedSchema", "renamedTable"));
    }

    @Test
    default void testListColumnsOneSchema()
            throws Exception
    {
        SchemaTableName schemaTableName1 = tableInDefaultSchema("table1");
        SchemaTableName schemaTableName2 = tableInDefaultSchema("table2");

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        List<ColumnMetadata> columns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        List<ColumnMetadata> expectedColumns = extendWithConnectorSpecificColumns(columns);

        ConnectorTableMetadata tableMetadata11 = new ConnectorTableMetadata(schemaTableName1, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata12 = new ConnectorTableMetadata(schemaTableName2, columns, getTableProperties());

        withTables(session, ImmutableList.of(tableMetadata11, tableMetadata12),
                ImmutableList.of(
                        metadata -> assertEquals(metadata.listTableColumns(session, new SchemaTablePrefix()),
                                ImmutableMap.of(
                                        schemaTableName1, expectedColumns,
                                        schemaTableName2, expectedColumns)),
                        metadata -> assertEquals(metadata.listTableColumns(session, prefixOf(schemaTableName1)),
                                ImmutableMap.of(
                                        schemaTableName1, expectedColumns))));
    }

    @Test
    @RequiredFeatures({CREATE_SCHEMA, DROP_SCHEMA})
    default void testListColumnsMultiSchema()
            throws Exception
    {
        String schemaName1 = "testListColumns1";
        String schemaName2 = "testListColumns2";
        SchemaTableName schemaTableName11 = tableInSpecificSchema(schemaName1, "table1");
        SchemaTableName schemaTableName12 = tableInSpecificSchema(schemaName1, "table2");
        SchemaTableName schemaTableName2 = tableInSpecificSchema(schemaName2, "table");

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        List<ColumnMetadata> columns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        List<ColumnMetadata> expectedColumns = extendWithConnectorSpecificColumns(columns);

        ConnectorTableMetadata tableMetadata11 = new ConnectorTableMetadata(schemaTableName11, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata12 = new ConnectorTableMetadata(schemaTableName12, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata2 = new ConnectorTableMetadata(schemaTableName2, columns, getTableProperties());

        withTables(session, ImmutableList.of(tableMetadata11, tableMetadata12, tableMetadata2),
                ImmutableList.of(
                        metadata -> assertEquals(metadata.listTableColumns(session, new SchemaTablePrefix()),
                                ImmutableMap.of(
                                        schemaTableName11, expectedColumns,
                                        schemaTableName12, expectedColumns,
                                        schemaTableName2, expectedColumns)),
                        metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName2)),
                                ImmutableMap.of(
                                        schemaTableName2, expectedColumns))));
    }

    @Test
    @RequiredFeatures(ADD_COLUMN)
    default void testAddColumn()
            throws Exception
    {
        SchemaTableName schemaTableName = tableInDefaultSchema("table");
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        List<ColumnMetadata> initialColumns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        ColumnMetadata newColumn = new ColumnMetadata("varchar_column", VARCHAR);

        List<ColumnMetadata> expectedInitialColumns = extendWithConnectorSpecificColumns(initialColumns);

        List<ColumnMetadata> expectedFinalColumns = extendWithConnectorSpecificColumns(
                ImmutableList.<ColumnMetadata>builder()
                        .addAll(initialColumns)
                        .add(newColumn)
                        .build());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, initialColumns, getTableProperties());

        withTables(session, ImmutableList.of(tableMetadata),
                ImmutableList.of(
                        metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName)),
                                ImmutableMap.of(schemaTableName, expectedInitialColumns)),
                        metadata -> metadata.addColumn(session, metadata.getTableHandle(session, schemaTableName), newColumn),
                        metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName)),
                                ImmutableMap.of(schemaTableName, expectedFinalColumns))));
    }

    @Test
    @RequiredFeatures(RENAME_COLUMN)
    default void testRenameColumn()
            throws Exception
    {
        SchemaTableName schemaTableName = tableInDefaultSchema("table");
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        ColumnMetadata unchanging = new ColumnMetadata("bigint_column", BIGINT);
        ColumnMetadata initial = new ColumnMetadata("initial_column", DOUBLE);
        ColumnMetadata renamed = new ColumnMetadata("renamed_column", DOUBLE);

        List<ColumnMetadata> initialColumns = ImmutableList.of(unchanging, initial);

        List<ColumnMetadata> expectedInitialColumns = extendWithConnectorSpecificColumns(initialColumns);

        List<ColumnMetadata> expectedFinalColumns = extendWithConnectorSpecificColumns(
                ImmutableList.<ColumnMetadata>builder()
                        .add(unchanging)
                        .add(renamed)
                        .build());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, initialColumns, getTableProperties());

        withTables(session, ImmutableList.of(tableMetadata),
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
                                ImmutableMap.of(schemaTableName, expectedFinalColumns))));
    }
}
