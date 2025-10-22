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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestMemoryMetadata
{
    private MemoryMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        metadata = new MemoryMetadata(new TestingNodeManager(), new MemoryConnectorId("test"));
    }

    @Test
    public void tableIsCreatedAfterCommits()
    {
        assertNoTables();

        SchemaTableName schemaTableName = new SchemaTableName("default", "temp_table");

        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(schemaTableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty());

        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.empty());
        assertTrue(tables.size() == 1, "Expected only one table");
        assertTrue(tables.get(0).getTableName().equals("temp_table"), "Expected table with name 'temp_table'");
    }

    @Test
    public void tableAlreadyExists()
    {
        assertNoTables();

        SchemaTableName test1Table = new SchemaTableName("default", "test1");
        SchemaTableName test2Table = new SchemaTableName("default", "test2");
        metadata.createTable(SESSION, new ConnectorTableMetadata(test1Table, ImmutableList.of()), false);

        try {
            metadata.createTable(SESSION, new ConnectorTableMetadata(test1Table, ImmutableList.of()), false);
            fail("Should fail because table already exists");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), ALREADY_EXISTS.toErrorCode());
            assertEquals(ex.getMessage(), "Table [default.test1] already exists");
        }

        ConnectorTableHandle test1TableHandle = metadata.getTableHandle(SESSION, test1Table);
        metadata.createTable(SESSION, new ConnectorTableMetadata(test2Table, ImmutableList.of()), false);

        try {
            metadata.renameTable(SESSION, test1TableHandle, test2Table);
            fail("Should fail because table already exists");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), ALREADY_EXISTS.toErrorCode());
            assertEquals(ex.getMessage(), "Table [default.test2] already exists");
        }
    }

    @Test
    public void testActiveTableIds()
    {
        assertNoTables();

        SchemaTableName firstTableName = new SchemaTableName("default", "first_table");
        metadata.createTable(SESSION, new ConnectorTableMetadata(firstTableName, ImmutableList.of(), ImmutableMap.of()), false);

        MemoryTableHandle firstTableHandle = (MemoryTableHandle) metadata.getTableHandle(SESSION, firstTableName);
        Long firstTableId = firstTableHandle.getTableId();

        assertTrue(metadata.beginInsert(SESSION, firstTableHandle).getActiveTableIds().contains(firstTableId));

        SchemaTableName secondTableName = new SchemaTableName("default", "second_table");
        metadata.createTable(SESSION, new ConnectorTableMetadata(secondTableName, ImmutableList.of(), ImmutableMap.of()), false);

        MemoryTableHandle secondTableHandle = (MemoryTableHandle) metadata.getTableHandle(SESSION, secondTableName);
        Long secondTableId = secondTableHandle.getTableId();

        assertNotEquals(firstTableId, secondTableId);
        assertTrue(metadata.beginInsert(SESSION, secondTableHandle).getActiveTableIds().contains(firstTableId));
        assertTrue(metadata.beginInsert(SESSION, secondTableHandle).getActiveTableIds().contains(secondTableId));
    }

    @Test
    public void testReadTableBeforeCreationCompleted()
    {
        assertNoTables();

        SchemaTableName tableName = new SchemaTableName("default", "temp_table");

        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(tableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty());

        List<SchemaTableName> tableNames = metadata.listTables(SESSION, Optional.empty());
        assertTrue(tableNames.size() == 1, "Expected exactly one table");

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, tableName);
        ConnectorTableLayoutResult tableLayoutResult = metadata.getTableLayoutForConstraint(SESSION, tableHandle, Constraint.alwaysTrue(), Optional.empty());
        assertTrue(tableLayoutResult != null, "Table layout is null.");
        ConnectorTableLayout tableLayout = tableLayoutResult.getTableLayout();
        ConnectorTableLayoutHandle tableLayoutHandle = tableLayout.getHandle();
        assertTrue(tableLayoutHandle instanceof MemoryTableLayoutHandle);
        assertTrue(((MemoryTableLayoutHandle) tableLayoutHandle).getDataFragments().isEmpty(), "Data fragments should be empty");

        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());
    }

    @Test
    public void testCreateSchema()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default"));
        metadata.createSchema(SESSION, "test", ImmutableMap.of());
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default", "test"));
        assertEquals(metadata.listTables(SESSION, "test"), ImmutableList.of());

        SchemaTableName tableName = new SchemaTableName("test", "first_table");
        metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        tableName,
                        ImmutableList.of(),
                        ImmutableMap.of()),
                false);

        assertEquals(metadata.listTables(SESSION, Optional.empty()), ImmutableList.of(tableName));
        assertEquals(metadata.listTables(SESSION, Optional.of("test")), ImmutableList.of(tableName));
        assertEquals(metadata.listTables(SESSION, Optional.of("default")), ImmutableList.of());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "View already exists: test\\.test_view")
    public void testCreateViewWithoutReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");
        ConnectorTableMetadata viewMetadata = new ConnectorTableMetadata(
                test,
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));
        metadata.createSchema(SESSION, "test", ImmutableMap.of());
        try {
            metadata.createView(SESSION, viewMetadata, "test", false);
        }
        catch (Exception e) {
            fail("should have succeeded");
        }
        metadata.createView(SESSION, viewMetadata, "test", false);
    }

    @Test
    public void testCreateViewWithReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");
        ConnectorTableMetadata viewMetadata = new ConnectorTableMetadata(
                test,
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));

        metadata.createSchema(SESSION, "test", ImmutableMap.of());
        metadata.createView(SESSION, viewMetadata, "aaa", true);
        metadata.createView(SESSION, viewMetadata, "bbb", true);

        assertEquals(metadata.getViews(SESSION, test.toSchemaTablePrefix()).get(test).getViewData(), "bbb");
    }

    @Test
    public void testViews()
    {
        SchemaTableName test1 = new SchemaTableName("test", "test_view1");
        ConnectorTableMetadata viewMetadata1 = new ConnectorTableMetadata(
                test1,
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));
        SchemaTableName test2 = new SchemaTableName("test", "test_view2");
        ConnectorTableMetadata viewMetadata2 = new ConnectorTableMetadata(
                test2,
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));

        SchemaTableName test3 = new SchemaTableName("test", "test_view3");

        // create schema
        metadata.createSchema(SESSION, "test", ImmutableMap.of());

        // create views
        metadata.createView(SESSION, viewMetadata1, "test1", false);
        metadata.createView(SESSION, viewMetadata2, "test2", false);

        // verify listing
        List<SchemaTableName> list = metadata.listViews(SESSION, "test");
        assertEqualsIgnoreOrder(list, ImmutableList.of(test1, test2));

        // verify getting data
        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(SESSION, new SchemaTablePrefix("test"));
        assertEquals(views.keySet(), ImmutableSet.of(test1, test2));
        assertEquals(views.get(test1).getViewData(), "test1");
        assertEquals(views.get(test2).getViewData(), "test2");

        // all schemas
        Map<SchemaTableName, ConnectorViewDefinition> allViews = metadata.getViews(SESSION, new SchemaTablePrefix());
        assertEquals(allViews.keySet(), ImmutableSet.of(test1, test2));

        // exact match on one schema and table
        Map<SchemaTableName, ConnectorViewDefinition> exactMatchView = metadata.getViews(SESSION, new SchemaTablePrefix("test", "test_view1"));
        assertEquals(exactMatchView.keySet(), ImmutableSet.of(test1));

        // non-existent table
        Map<SchemaTableName, ConnectorViewDefinition> nonexistentTableView = metadata.getViews(SESSION, new SchemaTablePrefix("test", "nonexistenttable"));
        assertTrue(nonexistentTableView.isEmpty());

        // non-existent schema
        Map<SchemaTableName, ConnectorViewDefinition> nonexistentSchemaView = metadata.getViews(SESSION, new SchemaTablePrefix("nonexistentschema"));
        assertTrue(nonexistentSchemaView.isEmpty());

        // drop first view
        metadata.dropView(SESSION, test1);

        views = metadata.getViews(SESSION, new SchemaTablePrefix("test"));
        assertEquals(views.keySet(), ImmutableSet.of(test2));

        // rename second view
        metadata.renameView(SESSION, test2, test3);

        Map<?, ?> result = metadata.getViews(SESSION, new SchemaTablePrefix("test"));
        assertTrue(result.containsKey(test3));

        // drop second view
        metadata.dropView(SESSION, test3);

        views = metadata.getViews(SESSION, new SchemaTablePrefix("test"));
        assertTrue(views.isEmpty());

        // verify listing everything
        views = metadata.getViews(SESSION, new SchemaTablePrefix());
        assertTrue(views.isEmpty());
    }

    @Test
    public void testCreateTableAndViewInNotExistSchema()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default"));

        SchemaTableName table1 = new SchemaTableName("test1", "test_schema_table1");
        try {
            metadata.beginCreateTable(SESSION, new ConnectorTableMetadata(table1, ImmutableList.of(), ImmutableMap.of()), Optional.empty());
            fail("Should fail because schema does not exist");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(ex.getMessage(), "Schema test1 not found");
        }
        assertNull(metadata.getTableHandle(SESSION, table1));

        SchemaTableName view2 = new SchemaTableName("test2", "test_schema_view2");
        ConnectorTableMetadata viewMetadata2 = new ConnectorTableMetadata(
                view2,
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));
        try {
            metadata.createView(SESSION, viewMetadata2, "aaa", false);
            fail("Should fail because schema does not exist");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(ex.getMessage(), "Schema test2 not found");
        }
        assertNull(metadata.getTableHandle(SESSION, view2));

        SchemaTableName view3 = new SchemaTableName("test3", "test_schema_view3");
        ConnectorTableMetadata viewMetadata3 = new ConnectorTableMetadata(
                view3,
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));

        try {
            metadata.createView(SESSION, viewMetadata3, "bbb", true);
            fail("Should fail because schema does not exist");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(ex.getMessage(), "Schema test3 not found");
        }
        assertNull(metadata.getTableHandle(SESSION, view3));

        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default"));
    }

    @Test
    public void testRenameTable()
    {
        SchemaTableName tableName = new SchemaTableName("test_schema", "test_table_to_be_renamed");
        metadata.createSchema(SESSION, "test_schema", ImmutableMap.of());
        ConnectorOutputTableHandle table = metadata.beginCreateTable(
                SESSION,
                new ConnectorTableMetadata(tableName, ImmutableList.of(), ImmutableMap.of()),
                Optional.empty());
        metadata.finishCreateTable(SESSION, table, ImmutableList.of(), ImmutableList.of());

        // rename table to schema which does not exist
        SchemaTableName invalidSchemaTableName = new SchemaTableName("test_schema_not_exist", "test_table_renamed");
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, tableName);
        Throwable throwable = expectThrows(SchemaNotFoundException.class, () -> metadata.renameTable(SESSION, tableHandle, invalidSchemaTableName));
        assertTrue(throwable.getMessage().equals("Schema test_schema_not_exist not found"));

        // rename table to same schema
        SchemaTableName sameSchemaTableName = new SchemaTableName("test_schema", "test_renamed");
        metadata.renameTable(SESSION, metadata.getTableHandle(SESSION, tableName), sameSchemaTableName);
        assertEquals(metadata.listTables(SESSION, "test_schema"), ImmutableList.of(sameSchemaTableName));

        // rename table to different schema
        metadata.createSchema(SESSION, "test_different_schema", ImmutableMap.of());
        SchemaTableName differentSchemaTableName = new SchemaTableName("test_different_schema", "test_renamed");
        metadata.renameTable(SESSION, metadata.getTableHandle(SESSION, sameSchemaTableName), differentSchemaTableName);
        assertEquals(metadata.listTables(SESSION, "test_schema"), ImmutableList.of());
        assertEquals(metadata.listTables(SESSION, "test_different_schema"), ImmutableList.of(differentSchemaTableName));
    }

    private void assertNoTables()
    {
        assertEquals(metadata.listTables(SESSION, Optional.empty()), ImmutableList.of(), "No table was expected");
    }
}
