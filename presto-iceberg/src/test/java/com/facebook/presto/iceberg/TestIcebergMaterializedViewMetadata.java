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
package com.facebook.presto.iceberg;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_BASE_TABLES;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_COLUMN_MAPPINGS;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_FORMAT_VERSION;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_LAST_REFRESH_SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_ORIGINAL_SQL;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_OWNER;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_SECURITY_MODE;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_STORAGE_SCHEMA;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_STORAGE_TABLE_NAME;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestIcebergMaterializedViewMetadata
        extends AbstractTestQueryFramework
{
    private File warehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();

        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();

        serverUri = restServer.getBaseUrl().toString();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(serverUri))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .setExtraProperties(ImmutableMap.of("experimental.legacy-materialized-views", "false"))
                .build().getQueryRunner();
    }

    @Test
    public void testMaterializedViewSnapshotTracking()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_snapshot_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_snapshot_base VALUES (1, 100)", 1);

        assertUpdate("CREATE MATERIALIZED VIEW test_snapshot_mv AS SELECT id, value FROM test_snapshot_base");

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId = TableIdentifier.of(Namespace.of("test_schema"), "test_snapshot_mv");

            View viewBeforeRefresh = catalog.loadView(viewId);
            String lastRefreshBefore = viewBeforeRefresh.properties().get(PRESTO_MATERIALIZED_VIEW_LAST_REFRESH_SNAPSHOT_ID);
            assertNull(lastRefreshBefore, "Expected last_refresh_snapshot_id to be null before refresh");

            assertUpdate("REFRESH MATERIALIZED VIEW test_snapshot_mv", 1);

            View viewAfterRefresh = catalog.loadView(viewId);
            String lastRefreshAfter = viewAfterRefresh.properties().get(PRESTO_MATERIALIZED_VIEW_LAST_REFRESH_SNAPSHOT_ID);
            assertNotNull(lastRefreshAfter, "Expected last_refresh_snapshot_id to be set after refresh");

            String baseSnapshot = viewAfterRefresh.properties().get("presto.materialized_view.base_snapshot.test_schema.test_snapshot_base");
            assertNotNull(baseSnapshot, "Expected base table snapshot ID to be tracked");

            assertUpdate("INSERT INTO test_snapshot_base VALUES (2, 200)", 1);
            assertUpdate("REFRESH MATERIALIZED VIEW test_snapshot_mv", 2);

            View viewAfterSecondRefresh = catalog.loadView(viewId);
            String lastRefreshAfter2 = viewAfterSecondRefresh.properties().get(PRESTO_MATERIALIZED_VIEW_LAST_REFRESH_SNAPSHOT_ID);
            String baseSnapshot2 = viewAfterSecondRefresh.properties().get("presto.materialized_view.base_snapshot.test_schema.test_snapshot_base");

            assertNotEquals(lastRefreshAfter2, lastRefreshAfter, "Expected last_refresh_snapshot_id to change after second refresh");
            assertNotEquals(baseSnapshot2, baseSnapshot, "Expected base table snapshot ID to change after INSERT");

            assertUpdate("CREATE TABLE test_empty_snapshot (id BIGINT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_empty_mv AS SELECT id FROM test_empty_snapshot");
            assertUpdate("REFRESH MATERIALIZED VIEW test_empty_mv", 0);

            TableIdentifier emptyViewId = TableIdentifier.of(Namespace.of("test_schema"), "test_empty_mv");
            View emptyView = catalog.loadView(emptyViewId);
            String emptySnapshot = emptyView.properties().get("presto.materialized_view.base_snapshot.test_schema.test_empty_snapshot");

            assertEquals(emptySnapshot, "0", "Expected empty base table snapshot ID to be 0");

            assertUpdate("DROP MATERIALIZED VIEW test_empty_mv");
            assertUpdate("DROP TABLE test_empty_snapshot");
        }
        finally {
            catalog.close();
        }

        assertUpdate("DROP MATERIALIZED VIEW test_snapshot_mv");
        assertUpdate("DROP TABLE test_snapshot_base");
    }

    @Test
    public void testMaterializedViewWithComplexColumnMappings()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_mapping_orders (order_id BIGINT, customer_id BIGINT, product_id BIGINT, order_date DATE, amount BIGINT)");
        assertUpdate("CREATE TABLE test_mapping_customers (customer_id BIGINT, customer_name VARCHAR, region VARCHAR)");
        assertUpdate("CREATE TABLE test_mapping_products (product_id BIGINT, product_name VARCHAR, category VARCHAR)");

        assertUpdate("INSERT INTO test_mapping_orders VALUES (1, 100, 1, DATE '2024-01-01', 500), (2, 200, 2, DATE '2024-01-02', 750)", 2);
        assertUpdate("INSERT INTO test_mapping_customers VALUES (100, 'Alice', 'US'), (200, 'Bob', 'EU')", 2);
        assertUpdate("INSERT INTO test_mapping_products VALUES (1, 'Widget', 'Electronics'), (2, 'Gadget', 'Electronics')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mapping_mv AS " +
                "SELECT o.order_id, c.customer_name, c.region, o.order_date, o.amount, p.product_name, p.category " +
                "FROM test_mapping_orders o " +
                "JOIN test_mapping_customers c ON o.customer_id = c.customer_id " +
                "JOIN test_mapping_products p ON o.product_id = p.product_id");

        assertQuery("SELECT COUNT(*) FROM test_mapping_mv", "SELECT 2");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mapping_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mapping_mv\"", "SELECT 2");

        assertQuery("SELECT order_id, customer_name, region FROM test_mapping_mv WHERE order_id = 1",
                "VALUES (1, 'Alice', 'US')");

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId = TableIdentifier.of(Namespace.of("test_schema"), "test_mapping_mv");
            View view = catalog.loadView(viewId);

            String columnMappings = view.properties().get("presto.materialized_view.column_mappings");
            assertNotNull(columnMappings, "Expected column_mappings property to be set");

            assertFalse(columnMappings.isEmpty() || columnMappings.equals("[]"), "Expected non-empty column mappings for multi-table join");
        }
        finally {
            catalog.close();
        }

        assertUpdate("INSERT INTO test_mapping_orders VALUES (3, 100, 1, DATE '2024-01-03', 1000)", 1);
        assertUpdate("REFRESH MATERIALIZED VIEW test_mapping_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mapping_mv\"", "SELECT 3");

        assertUpdate("DROP MATERIALIZED VIEW test_mapping_mv");
        assertUpdate("DROP TABLE test_mapping_products");
        assertUpdate("DROP TABLE test_mapping_customers");
        assertUpdate("DROP TABLE test_mapping_orders");
    }

    @Test
    public void testMaterializedViewWithSpecialCharactersInTableNames()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_base_123 (id BIGINT, value_1 BIGINT)");
        assertUpdate("CREATE TABLE test_base_456_special (id BIGINT, value_2 BIGINT)");

        assertUpdate("INSERT INTO test_base_123 VALUES (1, 100), (2, 200)", 2);
        assertUpdate("INSERT INTO test_base_456_special VALUES (1, 300), (2, 400)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_special_chars_mv AS " +
                "SELECT a.id, a.value_1, b.value_2 " +
                "FROM test_base_123 a " +
                "JOIN test_base_456_special b ON a.id = b.id");

        assertQuery("SELECT COUNT(*) FROM test_special_chars_mv", "SELECT 2");
        assertQuery("SELECT * FROM test_special_chars_mv ORDER BY id",
                "VALUES (1, 100, 300), (2, 200, 400)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_special_chars_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_special_chars_mv\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_special_chars_mv\" ORDER BY id",
                "VALUES (1, 100, 300), (2, 200, 400)");

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId = TableIdentifier.of(Namespace.of("test_schema"), "test_special_chars_mv");
            View view = catalog.loadView(viewId);

            String baseTables = view.properties().get("presto.materialized_view.base_tables");
            assertNotNull(baseTables, "Expected base_tables property to be set");

            assertTrue(baseTables.contains("test_base_123"), "Expected base_tables to contain 'test_base_123', got: " + baseTables);
            assertTrue(baseTables.contains("test_base_456_special"), "Expected base_tables to contain 'test_base_456_special', got: " + baseTables);

            String snapshot1 = view.properties().get("presto.materialized_view.base_snapshot.test_schema.test_base_123");
            String snapshot2 = view.properties().get("presto.materialized_view.base_snapshot.test_schema.test_base_456_special");

            assertNotNull(snapshot1, "Expected snapshot for test_base_123 to be tracked");
            assertNotNull(snapshot2, "Expected snapshot for test_base_456_special to be tracked");
        }
        finally {
            catalog.close();
        }

        assertUpdate("INSERT INTO test_base_123 VALUES (3, 500)", 1);
        assertUpdate("INSERT INTO test_base_456_special VALUES (3, 600)", 1);

        assertQuery("SELECT COUNT(*) FROM test_special_chars_mv", "SELECT 3");

        assertUpdate("REFRESH MATERIALIZED VIEW test_special_chars_mv", 3);
        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_special_chars_mv\"", "SELECT 3");

        assertUpdate("DROP MATERIALIZED VIEW test_special_chars_mv");
        assertUpdate("DROP TABLE test_base_456_special");
        assertUpdate("DROP TABLE test_base_123");
    }

    @Test
    public void testMaterializedViewOtherValidationErrors()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_other_validation_base (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_other_validation_base VALUES (1, 'Alice')", 1);

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId1 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_empty_base_tables");
            Map<String, String> properties = new HashMap<>();
            properties.put(PRESTO_MATERIALIZED_VIEW_FORMAT_VERSION, CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION + "");
            properties.put(PRESTO_MATERIALIZED_VIEW_ORIGINAL_SQL, "SELECT 1 as id");
            properties.put(PRESTO_MATERIALIZED_VIEW_STORAGE_SCHEMA, "test_schema");
            properties.put(PRESTO_MATERIALIZED_VIEW_STORAGE_TABLE_NAME, "storage_empty_base");
            properties.put(PRESTO_MATERIALIZED_VIEW_COLUMN_MAPPINGS, "[]");
            properties.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, "");
            properties.put(PRESTO_MATERIALIZED_VIEW_OWNER, "test_user");
            properties.put(PRESTO_MATERIALIZED_VIEW_SECURITY_MODE, "DEFINER");

            assertUpdate("CREATE TABLE storage_empty_base (id BIGINT)");

            catalog.buildView(viewId1)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT 1 as id")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties)
                    .create();

            assertQuery("SELECT * FROM test_mv_empty_base_tables", "SELECT 1");

            catalog.dropView(viewId1);
            assertUpdate("DROP TABLE storage_empty_base");

            TableIdentifier viewId2 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_invalid_json");
            Map<String, String> properties2 = new HashMap<>();
            properties2.put(PRESTO_MATERIALIZED_VIEW_FORMAT_VERSION, CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION + "");
            properties2.put(PRESTO_MATERIALIZED_VIEW_ORIGINAL_SQL, "SELECT id FROM test_other_validation_base");
            properties2.put(PRESTO_MATERIALIZED_VIEW_STORAGE_SCHEMA, "test_schema");
            properties2.put(PRESTO_MATERIALIZED_VIEW_STORAGE_TABLE_NAME, "storage_invalid_json");
            properties2.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, "[{\"schema\":\"test_schema\", \"table\": \"test_other_validation_base\"}]");
            properties2.put(PRESTO_MATERIALIZED_VIEW_COLUMN_MAPPINGS, "{invalid json here");
            properties2.put(PRESTO_MATERIALIZED_VIEW_OWNER, "test_user");
            properties2.put(PRESTO_MATERIALIZED_VIEW_SECURITY_MODE, "DEFINER");

            assertUpdate("CREATE TABLE storage_invalid_json (id BIGINT)");

            catalog.buildView(viewId2)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT id FROM test_other_validation_base")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties2)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_invalid_json",
                    ".*Invalid JSON string.*");

            catalog.dropView(viewId2);
            assertUpdate("DROP TABLE storage_invalid_json");

            TableIdentifier viewId3 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_nonexistent_base");
            Map<String, String> properties3 = new HashMap<>();
            properties3.put(PRESTO_MATERIALIZED_VIEW_FORMAT_VERSION, CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION + "");
            properties3.put(PRESTO_MATERIALIZED_VIEW_ORIGINAL_SQL, "SELECT id FROM nonexistent_table");
            properties3.put(PRESTO_MATERIALIZED_VIEW_STORAGE_SCHEMA, "test_schema");
            properties3.put(PRESTO_MATERIALIZED_VIEW_STORAGE_TABLE_NAME, "storage_nonexistent_base");
            properties3.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, "[{\"schema\":\"test_schema\", \"table\": \"nonexistent_table\"}]");
            properties3.put(PRESTO_MATERIALIZED_VIEW_COLUMN_MAPPINGS, "[]");
            properties3.put(PRESTO_MATERIALIZED_VIEW_OWNER, "test_user");
            properties3.put(PRESTO_MATERIALIZED_VIEW_SECURITY_MODE, "DEFINER");

            assertUpdate("CREATE TABLE storage_nonexistent_base (id BIGINT)");

            catalog.buildView(viewId3)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT id FROM nonexistent_table")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties3)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_nonexistent_base",
                    ".*(does not exist|not found).*");

            catalog.dropView(viewId3);
            assertUpdate("DROP TABLE storage_nonexistent_base");
        }
        finally {
            catalog.close();
        }

        assertUpdate("CREATE TABLE existing_storage_table (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO existing_storage_table VALUES (1, 100)", 1);

        assertQueryFails("CREATE MATERIALIZED VIEW test_mv_duplicate_storage " +
                        "WITH (materialized_view_storage_table_name = 'existing_storage_table') " +
                        "AS SELECT id, name FROM test_other_validation_base",
                ".*already exists.*");

        assertUpdate("DROP TABLE existing_storage_table");
        assertUpdate("DROP TABLE test_other_validation_base");
    }

    @Test
    public void testMaterializedViewInvalidBaseTableNameFormat()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_format_base (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_format_base VALUES (1, 'Alice')", 1);

        assertUpdate("CREATE TABLE storage_table_1 (id BIGINT)");
        assertUpdate("CREATE TABLE storage_table_2 (id BIGINT)");
        assertUpdate("CREATE TABLE storage_table_3 (id BIGINT)");
        assertUpdate("CREATE TABLE storage_table_4 (id BIGINT)");
        assertUpdate("CREATE TABLE storage_table_5 (id BIGINT)");
        assertUpdate("CREATE TABLE storage_table_6 (id BIGINT)");

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId1 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_no_schema");
            Map<String, String> properties1 = createValidMvProperties("storage_table_1");
            properties1.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, "table_only");

            catalog.buildView(viewId1)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT id FROM table_only")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties1)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_no_schema",
                    ".*Invalid base table name format: table_only.*");

            catalog.dropView(viewId1);

            TableIdentifier viewId2 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_empty_schema");
            Map<String, String> properties2 = createValidMvProperties("storage_table_2");
            properties2.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, "schema.");

            catalog.buildView(viewId2)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT id FROM schema.")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties2)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_empty_schema",
                    ".*Invalid base table name format: schema\\..*");

            catalog.dropView(viewId2);

            TableIdentifier viewId3 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_empty_table");
            Map<String, String> properties3 = createValidMvProperties("storage_table_3");
            properties3.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, ".table");

            catalog.buildView(viewId3)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT id FROM .table")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties3)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_empty_table",
                    ".*Invalid base table name format: \\.table.*");

            catalog.dropView(viewId3);

            TableIdentifier viewId4 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_double_dots");
            Map<String, String> properties4 = createValidMvProperties("storage_table_4");
            properties4.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, "schema..table");

            catalog.buildView(viewId4)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT id FROM schema..table")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties4)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_double_dots",
                    ".*Invalid base table name format: schema\\.\\.table.*");

            catalog.dropView(viewId4);

            TableIdentifier viewId5 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_too_many_parts");
            Map<String, String> properties5 = createValidMvProperties("storage_table_5");
            properties5.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, "a.b.c");

            catalog.buildView(viewId5)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT id FROM a.b.c")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties5)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_too_many_parts",
                    ".*Invalid base table name format: a\\.b\\.c.*");

            catalog.dropView(viewId5);

            TableIdentifier viewId6 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_no_separator");
            Map<String, String> properties6 = createValidMvProperties("storage_table_6");
            properties6.put(PRESTO_MATERIALIZED_VIEW_BASE_TABLES, "schema_table");

            catalog.buildView(viewId6)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withQuery("spark", "SELECT id FROM test_schema.test_format_base")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties6)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_no_separator",
                    ".*Invalid base table name format: schema_table.*");

            catalog.dropView(viewId6);
        }
        finally {
            catalog.close();
        }

        assertUpdate("DROP TABLE test_format_base");
    }

    @Test
    public void testMaterializedViewMissingRequiredProperties()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_validation_base (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_validation_base VALUES (1, 'Alice')", 1);

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId1 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_missing_base_tables");
            Map<String, String> properties1 = new HashMap<>();
            properties1.put("presto.materialized_view.format_version", CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION + "");
            properties1.put("presto.materialized_view.original_sql", "SELECT id, name FROM test_validation_base");
            properties1.put("presto.materialized_view.storage_schema", "test_schema");
            properties1.put("presto.materialized_view.storage_table_name", "storage1");
            properties1.put("presto.materialized_view.column_mappings", "[]");

            catalog.buildView(viewId1)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get()),
                            Types.NestedField.required(2, "name", Types.StringType.get())))
                    .withQuery("spark", "SELECT id, name FROM test_validation_base")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties1)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_missing_base_tables",
                    ".*Materialized view missing required property: presto.materialized_view.base_tables.*");

            catalog.dropView(viewId1);

            TableIdentifier viewId2 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_missing_column_mappings");
            Map<String, String> properties2 = new HashMap<>();
            properties2.put("presto.materialized_view.format_version", CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION + "");
            properties2.put("presto.materialized_view.original_sql", "SELECT id, name FROM test_validation_base");
            properties2.put("presto.materialized_view.storage_schema", "test_schema");
            properties2.put("presto.materialized_view.storage_table_name", "storage2");
            properties2.put("presto.materialized_view.base_tables", "[{\"schema\":\"test_schema\", \"table\": \"test_validation_base\"}]");

            catalog.buildView(viewId2)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get()),
                            Types.NestedField.required(2, "name", Types.StringType.get())))
                    .withQuery("spark", "SELECT id, name FROM test_validation_base")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties2)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_missing_column_mappings",
                    ".*Materialized view missing required property: presto.materialized_view.column_mappings.*");

            catalog.dropView(viewId2);

            TableIdentifier viewId3 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_missing_storage_schema");
            Map<String, String> properties3 = new HashMap<>();
            properties3.put("presto.materialized_view.format_version", CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION + "");
            properties3.put("presto.materialized_view.original_sql", "SELECT id, name FROM test_validation_base");
            properties3.put("presto.materialized_view.storage_table_name", "storage3");
            properties3.put("presto.materialized_view.base_tables", "[{\"schema\":\"test_schema\", \"table\": \"test_validation_base\"}]");
            properties3.put("presto.materialized_view.column_mappings", "[]");

            catalog.buildView(viewId3)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get()),
                            Types.NestedField.required(2, "name", Types.StringType.get())))
                    .withQuery("spark", "SELECT id, name FROM test_validation_base")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties3)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_missing_storage_schema",
                    ".*Materialized view missing required property: presto.materialized_view.storage_schema.*");

            catalog.dropView(viewId3);

            TableIdentifier viewId4 = TableIdentifier.of(Namespace.of("test_schema"), "test_mv_missing_storage_table_name");
            Map<String, String> properties4 = new HashMap<>();
            properties4.put("presto.materialized_view.format_version", CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION + "");
            properties4.put("presto.materialized_view.original_sql", "SELECT id, name FROM test_validation_base");
            properties4.put("presto.materialized_view.storage_schema", "test_schema");
            properties4.put("presto.materialized_view.base_tables", "[{\"schema\":\"test_schema\", \"table\": \"test_validation_base\"}]");
            properties4.put("presto.materialized_view.column_mappings", "[]");

            catalog.buildView(viewId4)
                    .withSchema(new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "id", Types.LongType.get()),
                            Types.NestedField.required(2, "name", Types.StringType.get())))
                    .withQuery("spark", "SELECT id, name FROM test_validation_base")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .withProperties(properties4)
                    .create();

            assertQueryFails("SELECT * FROM test_mv_missing_storage_table_name",
                    ".*Materialized view missing required property: presto.materialized_view.storage_table_name.*");

            catalog.dropView(viewId4);
        }
        finally {
            catalog.close();
        }

        assertUpdate("DROP TABLE test_validation_base");
    }

    private Map<String, String> createValidMvProperties(String storageTableName)
    {
        Map<String, String> properties = new HashMap<>();
        properties.put(PRESTO_MATERIALIZED_VIEW_FORMAT_VERSION, CURRENT_MATERIALIZED_VIEW_FORMAT_VERSION + "");
        properties.put(PRESTO_MATERIALIZED_VIEW_ORIGINAL_SQL, "SELECT id FROM test_format_base");
        properties.put(PRESTO_MATERIALIZED_VIEW_STORAGE_SCHEMA, "test_schema");
        properties.put(PRESTO_MATERIALIZED_VIEW_STORAGE_TABLE_NAME, storageTableName);
        properties.put(PRESTO_MATERIALIZED_VIEW_COLUMN_MAPPINGS, "[]");
        properties.put(PRESTO_MATERIALIZED_VIEW_OWNER, "test_user");
        properties.put(PRESTO_MATERIALIZED_VIEW_SECURITY_MODE, "DEFINER");
        return properties;
    }
}
