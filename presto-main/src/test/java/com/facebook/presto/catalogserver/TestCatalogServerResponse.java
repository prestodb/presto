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
package com.facebook.presto.catalogserver;

import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.connector.informationSchema.InformationSchemaTableHandle;
import com.facebook.presto.connector.informationSchema.InformationSchemaTransactionHandle;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.transaction.TransactionId;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCatalogServerResponse
{
    private TestingCatalogServerClient testingCatalogServerClient;
    private ObjectMapper objectMapper;

    @BeforeTest
    public void setup()
    {
        this.testingCatalogServerClient = new TestingCatalogServerClient();
        Injector injector = Guice.createInjector(new JsonModule(), new HandleJsonModule());
        this.objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @Test
    public void testSchemaExists()
    {
        MetadataEntry<Boolean> metadataEntry = testingCatalogServerClient.schemaExists(null, null, null);
        boolean schemaExists = metadataEntry.getValue();
        boolean actualSchemaExists = false;

        assertEquals(schemaExists, actualSchemaExists);
        assertTrue(metadataEntry.getIsJson());
    }

    @Test
    public void testCatalogExists()
    {
        MetadataEntry<Boolean> metadataEntry = testingCatalogServerClient.catalogExists(null, null, null);
        boolean catalogExists = metadataEntry.getValue();
        boolean actualCatalogExists = true;

        assertEquals(catalogExists, actualCatalogExists);
        assertTrue(metadataEntry.getIsJson());
    }

    @Test
    public void testListSchemaNames()
            throws Exception
    {
        MetadataEntry<String> metadataEntry = testingCatalogServerClient.listSchemaNames(null, null, null);
        List<String> schemaNames = objectMapper.readValue(metadataEntry.getValue(), new TypeReference<List<String>>() {});
        List<String> actualSchemaNames = ImmutableList.of(
                "information_schema",
                "tiny",
                "sf1",
                "sf100",
                "sf300",
                "sf1000",
                "sf3000",
                "sf10000",
                "sf30000",
                "sf100000");

        assertEquals(schemaNames, actualSchemaNames);
        assertFalse(metadataEntry.getIsJson());
    }

    @Test
    public void testGetTableHandle()
            throws Exception
    {
        MetadataEntry<String> metadataEntry = testingCatalogServerClient.getTableHandle(null, null, null);
        TableHandle tableHandle = objectMapper.readValue(metadataEntry.getValue(), TableHandle.class);
        ConnectorId connectorId = new ConnectorId("$info_schema@system");
        ConnectorTableHandle connectorHandle = new InformationSchemaTableHandle("system", "information_schema", "schemata");
        UUID uuid = UUID.fromString("ffe9ae3e-60de-4175-a0b5-d635767085fa");
        ConnectorTransactionHandle connectorTransactionHandle = new InformationSchemaTransactionHandle(new TransactionId(uuid));
        TableHandle actualTableHandle = new TableHandle(connectorId, connectorHandle, connectorTransactionHandle, Optional.empty());

        assertEquals(tableHandle, actualTableHandle);
        assertFalse(metadataEntry.getIsJson());
    }

    @Test
    public void testListTables()
            throws Exception
    {
        MetadataEntry<String> metadataEntry = testingCatalogServerClient.listTables(null, null, null);
        List<QualifiedObjectName> tableList = objectMapper.readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {});
        List<QualifiedObjectName> actualTableList = ImmutableList.of(new QualifiedObjectName("tpch", "sf1", "nation"));

        assertEquals(tableList, actualTableList);
        assertTrue(metadataEntry.getIsJson());
    }

    @Test
    public void testListViews()
            throws Exception
    {
        MetadataEntry<String> metadataEntry = testingCatalogServerClient.listViews(null, null, null);
        List<QualifiedObjectName> viewsList = objectMapper.readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {});
        List<QualifiedObjectName> actualViewsList = ImmutableList.of(
                new QualifiedObjectName("hive", "tpch", "eric"),
                new QualifiedObjectName("hive", "tpch", "eric2"));

        assertEquals(viewsList, actualViewsList);
        assertTrue(metadataEntry.getIsJson());
    }

    @Test
    public void testGetViews()
            throws Exception
    {
        MetadataEntry<String> metadataEntry = testingCatalogServerClient.getViews(null, null, null);
        Map<QualifiedObjectName, ViewDefinition> viewsMap =
                objectMapper.readValue(metadataEntry.getValue(), new TypeReference<Map<QualifiedObjectName, ViewDefinition>>() {});
        QualifiedObjectName key = new QualifiedObjectName("hive", "tpch", "eric");
        Map<QualifiedObjectName, ViewDefinition> actualViewsMap = ImmutableMap.of(
                key,
                new ViewDefinition(
                        "SELECT name\nFROM\n  tpch.sf1.nation\n",
                        Optional.of("hive"),
                        Optional.of("tpch"),
                        ImmutableList.of(),
                        Optional.of("ericn576"),
                        false));
        assertEquals(viewsMap.keySet(), actualViewsMap.keySet());
        ViewDefinition viewDefinition = viewsMap.get(key);
        ViewDefinition actualViewDefinition = actualViewsMap.get(key);

        assertEquals(viewDefinition.getOriginalSql(), actualViewDefinition.getOriginalSql());
        assertEquals(viewDefinition.getCatalog(), actualViewDefinition.getCatalog());
        assertEquals(viewDefinition.getSchema(), actualViewDefinition.getSchema());
        assertEquals(viewDefinition.getOwner(), actualViewDefinition.getOwner());
        assertFalse(metadataEntry.getIsJson());
    }

    @Test
    public void testGetView()
            throws Exception
    {
        MetadataEntry<String> metadataEntry = testingCatalogServerClient.getView(null, null, null);
        ViewDefinition viewDefinition = objectMapper.readValue(metadataEntry.getValue(), ViewDefinition.class);
        ViewDefinition actualViewDefinition = new ViewDefinition(
                "SELECT name\nFROM\n  tpch.sf1.nation\n",
                Optional.of("hive"),
                Optional.of("tpch"),
                ImmutableList.of(),
                Optional.of("ericn576"),
                false);

        assertEquals(viewDefinition.getOriginalSql(), actualViewDefinition.getOriginalSql());
        assertEquals(viewDefinition.getCatalog(), actualViewDefinition.getCatalog());
        assertEquals(viewDefinition.getSchema(), actualViewDefinition.getSchema());
        assertEquals(viewDefinition.getOwner(), actualViewDefinition.getOwner());
        assertFalse(metadataEntry.getIsJson());
    }

    @Test
    public void testGetMaterializedView()
            throws Exception
    {
        MetadataEntry<String> metadataEntry = testingCatalogServerClient.getMaterializedView(null, null, null);
        ConnectorMaterializedViewDefinition connectorMaterializedViewDefinition = objectMapper.readValue(
                metadataEntry.getValue(),
                ConnectorMaterializedViewDefinition.class);
        String originalSql = "SELECT\n  name\n, nationkey\nFROM\n  test_customer_base\n";
        String schema = "tpch";
        String table = "eric";
        List<SchemaTableName> baseTables = ImmutableList.of(new SchemaTableName("tpch", "test_customer_base"));
        Optional<String> owner = Optional.of("ericn576");
        ConnectorMaterializedViewDefinition.TableColumn tableColumn = new ConnectorMaterializedViewDefinition.TableColumn(
                new SchemaTableName("tpch", "eric"),
                "name",
                true);
        ConnectorMaterializedViewDefinition.TableColumn listTableColumn = new ConnectorMaterializedViewDefinition.TableColumn(
                new SchemaTableName("tpch", "test_customer_base"),
                "name",
                true);
        List<ConnectorMaterializedViewDefinition.ColumnMapping> columnMappings =
                ImmutableList.of(new ConnectorMaterializedViewDefinition.ColumnMapping(tableColumn, ImmutableList.of(listTableColumn)));
        List<SchemaTableName> baseTablesOnOuterJoinSide = ImmutableList.of();
        Optional<List<String>> validRefreshColumns = Optional.of(ImmutableList.of("nationkey"));
        ConnectorMaterializedViewDefinition actualConnectorMaterializedViewDefinition = new ConnectorMaterializedViewDefinition(
                originalSql,
                schema,
                table,
                baseTables,
                owner,
                columnMappings,
                baseTablesOnOuterJoinSide,
                validRefreshColumns);

        assertEquals(connectorMaterializedViewDefinition.getOriginalSql(), actualConnectorMaterializedViewDefinition.getOriginalSql());
        assertEquals(connectorMaterializedViewDefinition.getSchema(), actualConnectorMaterializedViewDefinition.getSchema());
        assertEquals(connectorMaterializedViewDefinition.getTable(), actualConnectorMaterializedViewDefinition.getTable());
        assertEquals(connectorMaterializedViewDefinition.getBaseTables(), actualConnectorMaterializedViewDefinition.getBaseTables());
        assertEquals(connectorMaterializedViewDefinition.getOwner(), actualConnectorMaterializedViewDefinition.getOwner());
        assertEquals(connectorMaterializedViewDefinition.getColumnMappingsAsMap(), actualConnectorMaterializedViewDefinition.getColumnMappingsAsMap());
        assertEquals(connectorMaterializedViewDefinition.getBaseTablesOnOuterJoinSide(), actualConnectorMaterializedViewDefinition.getBaseTablesOnOuterJoinSide());
        assertEquals(connectorMaterializedViewDefinition.getValidRefreshColumns(), actualConnectorMaterializedViewDefinition.getValidRefreshColumns());
        assertTrue(metadataEntry.getIsJson());
    }

    @Test
    public void testGetReferencedMaterializedViews()
            throws Exception
    {
        MetadataEntry<String> metadataEntry = testingCatalogServerClient.getReferencedMaterializedViews(null, null, null);
        List<QualifiedObjectName> referencedMaterializedViewsList = objectMapper.readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {});
        List<QualifiedObjectName> actualReferencedMaterializedViewsList = ImmutableList.of(
                new QualifiedObjectName("hive", "tpch", "test_customer_base"));

        assertEquals(referencedMaterializedViewsList, actualReferencedMaterializedViewsList);
        assertTrue(metadataEntry.getIsJson());
    }
}
