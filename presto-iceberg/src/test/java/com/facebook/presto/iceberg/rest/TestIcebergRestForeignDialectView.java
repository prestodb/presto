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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

/**
 * Regression tests for views present in an Iceberg REST catalog that were not created by Presto
 * (e.g. by Netezza, Spark or Trino directly against the catalog), covering:
 * <a href="https://github.com/prestodb/presto/issues/28318">Presto Iceberg Rest Catalog - fails to read view representation</a>
 */
@Test(singleThreaded = true)
public class TestIcebergRestForeignDialectView
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

        getQueryRunner().execute("CREATE SCHEMA IF NOT EXISTS test_schema");
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
                .build().getQueryRunner();
    }

    private RESTCatalog getRestCatalog()
    {
        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);
        return catalog;
    }

    /**
     * Creates an Iceberg view directly through the REST catalog (bypassing Presto's CREATE VIEW),
     * simulating a view authored by a foreign engine (e.g. Netezza) with a non-"presto" SQL dialect
     * and without the "presto_view" property Presto sets on views it creates.
     */
    private void createForeignDialectView(RESTCatalog catalog, String viewName, String sql)
    {
        TableIdentifier viewId = TableIdentifier.of(Namespace.of("test_schema"), viewName);
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
        catalog.buildView(viewId)
                .withSchema(schema)
                .withQuery("netezza", sql)
                .withDefaultNamespace(Namespace.of("test_schema"))
                .create();
    }

    @Test
    public void testListViewsExcludesForeignDialectView()
            throws Exception
    {
        RESTCatalog catalog = getRestCatalog();
        try {
            assertUpdate("CREATE TABLE test_show_views_base (id BIGINT)");
            assertUpdate("CREATE VIEW test_show_views_presto AS SELECT id FROM test_show_views_base");
            createForeignDialectView(catalog, "test_show_views_foreign", "select * from \"test_show_views_base\"");

            // Listing views (via information_schema.views, which is backed by getViews()) should
            // include the Presto-authored view but silently skip the foreign-dialect one, rather
            // than crashing with "Invalid view JSON".
            assertQuery("SELECT table_name FROM information_schema.views " +
                            "WHERE table_schema = 'test_schema' AND table_name LIKE 'test_show_views%'",
                    "VALUES 'test_show_views_presto'");
        }
        finally {
            catalog.dropView(TableIdentifier.of(Namespace.of("test_schema"), "test_show_views_foreign"));
            catalog.close();
            assertUpdate("DROP VIEW IF EXISTS test_show_views_presto");
            assertUpdate("DROP TABLE IF EXISTS test_show_views_base");
        }
    }

    @Test
    public void testSelectFromForeignDialectViewDoesNotCrash()
            throws Exception
    {
        RESTCatalog catalog = getRestCatalog();
        try {
            assertUpdate("CREATE TABLE test_select_foreign_base (id BIGINT)");
            createForeignDialectView(catalog, "test_select_foreign_view", "select * from \"test_select_foreign_base\"");

            // Querying the foreign-dialect view directly must not crash with "Invalid view JSON".
            // Since the view is not surfaced by getViews(), Presto falls through to the table lookup
            // path and reports the view as a missing table/view, rather than crashing.
            assertQueryFails("SELECT * FROM test_schema.test_select_foreign_view",
                    ".*(does not exist|Table .* not found).*");
        }
        finally {
            catalog.dropView(TableIdentifier.of(Namespace.of("test_schema"), "test_select_foreign_view"));
            catalog.close();
            assertUpdate("DROP TABLE IF EXISTS test_select_foreign_base");
        }
    }

    @Test
    public void testSelectFromPrestoViewStillWorksAlongsideForeignDialectView()
            throws Exception
    {
        RESTCatalog catalog = getRestCatalog();
        try {
            assertUpdate("CREATE TABLE test_mixed_base (id BIGINT)");
            assertUpdate("INSERT INTO test_mixed_base VALUES (1), (2), (3)", 3);
            assertUpdate("CREATE VIEW test_mixed_presto_view AS SELECT id FROM test_mixed_base");
            createForeignDialectView(catalog, "test_mixed_foreign_view", "select * from \"test_mixed_base\"");

            // A Presto-authored view in the same schema as a foreign-dialect view must still be
            // queryable normally, whether looked up individually or via a bulk listing operation.
            assertQuery("SELECT * FROM test_mixed_presto_view", "VALUES 1, 2, 3");
            assertQuery("SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'test_mixed_presto_view'", "VALUES 1");
        }
        finally {
            catalog.dropView(TableIdentifier.of(Namespace.of("test_schema"), "test_mixed_foreign_view"));
            catalog.close();
            assertUpdate("DROP VIEW IF EXISTS test_mixed_presto_view");
            assertUpdate("DROP TABLE IF EXISTS test_mixed_base");
        }
    }

    @Test
    public void testForeignEngineLabelledPrestoDialectStillRejected()
            throws Exception
    {
        RESTCatalog catalog = getRestCatalog();
        try {
            assertUpdate("CREATE TABLE test_spoofed_base (id BIGINT)");

            // A foreign engine could label its representation's dialect as "presto" without the SQL
            // actually being a serialized Presto ViewDefinition. The "presto_view" property (set only
            // by Presto's own CREATE VIEW path) must still be required to accept the view, even when
            // the dialect alone claims to be "presto".
            TableIdentifier viewId = TableIdentifier.of(Namespace.of("test_schema"), "test_spoofed_view");
            catalog.buildView(viewId)
                    .withSchema(new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
                    .withQuery("presto", "select * from \"test_spoofed_base\"")
                    .withDefaultNamespace(Namespace.of("test_schema"))
                    .create();

            assertQueryFails("SELECT * FROM test_schema.test_spoofed_view",
                    ".*(does not exist|Table .* not found).*");
            assertQuery("SELECT COUNT(*) FROM information_schema.views " +
                    "WHERE table_schema = 'test_schema' AND table_name = 'test_spoofed_view'", "VALUES 0");
        }
        finally {
            catalog.dropView(TableIdentifier.of(Namespace.of("test_schema"), "test_spoofed_view"));
            catalog.close();
            assertUpdate("DROP TABLE IF EXISTS test_spoofed_base");
        }
    }

    @Test
    public void testInformationSchemaColumnsForForeignDialectView()
            throws Exception
    {
        RESTCatalog catalog = getRestCatalog();
        try {
            assertUpdate("CREATE TABLE test_view_foreign_base (id BIGINT)");
            createForeignDialectView(catalog, "test_view_foreign", "select * from \"test_view_foreign_base\"");

            // information_schema.columns filtered by an exact table_name (a query pattern commonly
            // issued by BI tools) resolves candidate names via InformationSchemaMetadata's
            // calculatePrefixesWithTableName(), which uses the same existence check as SHOW COLUMNS /
            // DESCRIBE (MetadataResolver.getView().isPresent() || getTableHandle().isPresent()). Since
            // the foreign-dialect view is not surfaced by getViews(), it is excluded from this
            // existence check and yields no rows here, rather than crashing on the underlying SQL
            // representation.
            assertQueryReturnsEmptyResult("SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'test_schema' AND table_name = 'test_view_foreign'");
        }
        finally {
            catalog.dropView(TableIdentifier.of(Namespace.of("test_schema"), "test_view_foreign"));
            catalog.close();
            assertUpdate("DROP TABLE IF EXISTS test_view_foreign_base");
        }
    }
}
