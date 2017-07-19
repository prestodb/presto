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
package com.facebook.presto.connector.unittests;

import com.facebook.presto.connector.ConnectorTestHelper;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.test.GeneratableConnectorTest;
import com.facebook.presto.test.RequiredFeatures;
import com.facebook.presto.test.UnsupportedMethodInterceptor;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.test.ConnectorFeature.ADD_COLUMN;
import static com.facebook.presto.test.ConnectorFeature.CREATE_SCHEMA;
import static com.facebook.presto.test.ConnectorFeature.CREATE_TABLE_AS;
import static com.facebook.presto.test.ConnectorFeature.DROP_SCHEMA;
import static com.facebook.presto.test.ConnectorFeature.DROP_TABLE;
import static com.facebook.presto.test.ConnectorFeature.RENAME_COLUMN;
import static com.facebook.presto.test.ConnectorFeature.RENAME_TABLE;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

@GeneratableConnectorTest(testName = "MetadataTable")
@Test(singleThreaded = true)
@RequiredFeatures(requiredFeatures = {CREATE_TABLE_AS, DROP_TABLE})
@Listeners({UnsupportedMethodInterceptor.class})
public abstract class MetadataTableTest
        extends BaseSPITest
{
    protected MetadataTableTest(ConnectorTestHelper helper, int unused)
            throws Exception
    {
        super(helper);
    }

    @AfterClass
    private void cleanUp()
    {
        connector.shutdown();
    }

    private void testRenameTable(SchemaTableName initial, SchemaTableName renamed)
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                initial,
                ImmutableList.of(
                        new ColumnMetadata("bigint_column", BIGINT),
                        new ColumnMetadata("double_column", DOUBLE)),
                getTableProperties());

        run(
                withSchema(session, distinctSchemas(initial, renamed),
                        metadata -> {
                            ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
                            metadata.finishCreateTable(session, handle, ImmutableList.of());
                        },
                        metadata -> metadata.renameTable(session, metadata.getTableHandle(session, initial), renamed),
                        metadata -> assertEquals(getOnlyElement(metadata.listTables(session, renamed.getSchemaName())), renamed),
                        metadata -> metadata.dropTable(session, metadata.getTableHandle(session, renamed))));
    }

    @Test
    @RequiredFeatures(requiredFeatures = {RENAME_TABLE})
    public void testRenameTableWithinSchema()
    {
        testRenameTable(
                schemaTableName("initial"),
                schemaTableName("renamed"));
    }

    @Test
    @RequiredFeatures(requiredFeatures = {CREATE_SCHEMA, DROP_SCHEMA})
    public void testRenameTableAcrossSchema()
    {
        testRenameTable(
                schemaTableName("initialSchema", "initialTable"),
                schemaTableName("renamedSchema", "renamedTable"));
    }

    @Test
    public void testListColumnsOneSchema()
    {
        SchemaTableName schemaTableName1 = schemaTableName("table1");
        SchemaTableName schemaTableName2 = schemaTableName("table2");

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        List<ColumnMetadata> columns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        List<ColumnMetadata> expectedColumns = withInternalColumns(columns);

        ConnectorTableMetadata tableMetadata11 = new ConnectorTableMetadata(schemaTableName1, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata12 = new ConnectorTableMetadata(schemaTableName2, columns, getTableProperties());

        run(
                withTableDropped(session, ImmutableList.of(tableMetadata11, tableMetadata12),
                        metadata -> assertEquals(metadata.listTableColumns(session, new SchemaTablePrefix()),
                                ImmutableMap.of(
                                        schemaTableName1, expectedColumns,
                                        schemaTableName2, expectedColumns)),
                        metadata -> assertEquals(metadata.listTableColumns(session, prefixOf(schemaTableName1)),
                                ImmutableMap.of(
                                        schemaTableName1, expectedColumns))));
    }

    @Test
    @RequiredFeatures(requiredFeatures = {CREATE_SCHEMA, DROP_SCHEMA})
    public void testListColumnsMultiSchema()
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

        List<ColumnMetadata> expectedColumns = withInternalColumns(columns);

        ConnectorTableMetadata tableMetadata11 = new ConnectorTableMetadata(schemaTableName11, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata12 = new ConnectorTableMetadata(schemaTableName12, columns, getTableProperties());
        ConnectorTableMetadata tableMetadata2 = new ConnectorTableMetadata(schemaTableName2, columns, getTableProperties());

        run(
                withTableDropped(session, ImmutableList.of(tableMetadata11, tableMetadata12, tableMetadata2),
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
    @RequiredFeatures(requiredFeatures = {ADD_COLUMN})
    public void testAddColumn()
    {
        SchemaTableName schemaTableName = schemaTableName("table");
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        List<ColumnMetadata> initialColumns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        ColumnMetadata newColumn = new ColumnMetadata("varchar_column", VARCHAR);

        List<ColumnMetadata> expectedInitialColumns = withInternalColumns(initialColumns);

        List<ColumnMetadata> expectedFinalColumns = withInternalColumns(
                ImmutableList.<ColumnMetadata>builder()
                        .addAll(initialColumns)
                        .add(newColumn)
                        .build());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, initialColumns, getTableProperties());

        run(
                withTableDropped(session, ImmutableList.of(tableMetadata),
                        metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName)),
                                ImmutableMap.of(schemaTableName, expectedInitialColumns)),
                        metadata -> metadata.addColumn(session, metadata.getTableHandle(session, schemaTableName), newColumn),
                        metadata -> assertEquals(metadata.listTableColumns(session, prefixOfSchemaName(schemaTableName)),
                                ImmutableMap.of(schemaTableName, expectedFinalColumns))));
    }

    @Test
    @RequiredFeatures(requiredFeatures = {RENAME_COLUMN})
    public void testRenameColumn()
    {
        SchemaTableName schemaTableName = schemaTableName("table");
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        ColumnMetadata unchanging = new ColumnMetadata("bigint_column", BIGINT);
        ColumnMetadata initial = new ColumnMetadata("initial_column", DOUBLE);
        ColumnMetadata renamed = new ColumnMetadata("renamed_column", DOUBLE);

        List<ColumnMetadata> initialColumns = ImmutableList.of(unchanging, initial);

        List<ColumnMetadata> expectedInitialColumns = withInternalColumns(initialColumns);

        List<ColumnMetadata> expectedFinalColumns = withInternalColumns(
                ImmutableList.of(unchanging, renamed));

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, initialColumns, getTableProperties());

        run(
                withTableDropped(session, ImmutableList.of(tableMetadata),
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
