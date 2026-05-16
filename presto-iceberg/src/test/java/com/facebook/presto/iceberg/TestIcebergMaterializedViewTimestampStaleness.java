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
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_CROSS_CATALOG_ENABLED;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_LAST_REFRESH_TIMESTAMP;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_USE_TIMESTAMP_BASED_STALENESS;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestIcebergMaterializedViewTimestampStaleness
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
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(serverUri))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(true)
                .setExtraProperties(ImmutableMap.of(
                        "experimental.legacy-materialized-views", "false",
                        "experimental.allow-legacy-materialized-views-toggle", "true"))
                .build()
                .getQueryRunner();

        return queryRunner;
    }

    @Test
    public void testTimestampBasedStalenessProperties()
            throws Exception
    {
        assertUpdate("CREATE MATERIALIZED VIEW test_timestamp_mv WITH (cross_catalog_materialized_views_enabled = true) AS SELECT regionkey, name, comment FROM tpch.sf1.region");

        assertUpdate("REFRESH MATERIALIZED VIEW test_timestamp_mv", 5);

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId = TableIdentifier.of(Namespace.of("test_schema"), "test_timestamp_mv");
            View view = catalog.loadView(viewId);

            String crossCatalogEnabled = view.properties().get(PRESTO_MATERIALIZED_VIEW_CROSS_CATALOG_ENABLED);
            assertEquals(crossCatalogEnabled, "true", "Expected use_timestamp_based_staleness to be true");

            String useTimestampBased = view.properties().get(PRESTO_MATERIALIZED_VIEW_USE_TIMESTAMP_BASED_STALENESS);
            assertEquals(useTimestampBased, "true", "Expected use_timestamp_based_staleness to be true");

            String lastRefreshTimestamp = view.properties().get(PRESTO_MATERIALIZED_VIEW_LAST_REFRESH_TIMESTAMP);
            assertNotNull(lastRefreshTimestamp, "Expected last_refresh_timestamp to be set after refresh");
        }
        finally {
            catalog.close();
        }

        assertUpdate("DROP MATERIALIZED VIEW test_timestamp_mv");
    }

    @Test
    public void testCrossCatalogMaterializedViewDisabledFails()
            throws Exception
    {
        assertQueryFails(
                "CREATE MATERIALIZED VIEW iceberg.test_schema.test_cross_catalog_fail_mv " +
                        "AS SELECT regionkey, name FROM tpch.sf1.region",
                "Cross-catalog materialized views are not enabled. Materialized view test_schema.test_cross_catalog_fail_mv cannot be created from a base table sf1.region in a different catalog tpch.");
    }

    @Test
    public void testCrossCatalogWithTimestampBasedStaleness()
            throws Exception
    {
        assertUpdate("CREATE MATERIALIZED VIEW iceberg.test_schema.test_cross_timestamp_mv " +
                "WITH (" +
                "cross_catalog_materialized_views_enabled = true, " +
                "use_timestamp_based_staleness = true " +
                ") " +
                "AS SELECT regionkey, name, comment FROM tpch.sf1.region");

        assertUpdate("REFRESH MATERIALIZED VIEW iceberg.test_schema.test_cross_timestamp_mv", 5);

        assertQuery("SELECT regionkey, name FROM test_cross_timestamp_mv WHERE regionkey = 1",
                "VALUES (1, 'AMERICA')");

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId = TableIdentifier.of(Namespace.of("test_schema"), "test_cross_timestamp_mv");
            View view = catalog.loadView(viewId);

            String crossCatalogEnabled = view.properties().get(PRESTO_MATERIALIZED_VIEW_CROSS_CATALOG_ENABLED);
            assertEquals(crossCatalogEnabled, "true", "Expected cross_catalog_enabled to be true");

            String useTimestampBased = view.properties().get(PRESTO_MATERIALIZED_VIEW_USE_TIMESTAMP_BASED_STALENESS);
            assertEquals(useTimestampBased, "true", "Expected use_timestamp_based_staleness to be true");

            String lastRefreshTimestamp = view.properties().get(PRESTO_MATERIALIZED_VIEW_LAST_REFRESH_TIMESTAMP);
            assertNotNull(lastRefreshTimestamp, "Expected last_refresh_timestamp to be set after refresh");
        }
        finally {
            catalog.close();
        }

        assertUpdate("DROP MATERIALIZED VIEW test_cross_timestamp_mv");
    }

    @Test
    public void testCrossCatalogWithTimestampBasedStalenessDisabledFails()
    {
        assertQueryFails(
                "CREATE MATERIALIZED VIEW iceberg.test_schema.test_cross_fail_mv " +
                        "WITH (" +
                        "cross_catalog_materialized_views_enabled = true, " +
                        "use_timestamp_based_staleness = false " +
                        ") " +
                        "AS SELECT regionkey, name, comment FROM tpch.sf1.region",
                "use_timestamp_based_staleness must be true when cross_catalog_materialized_views_enabled is true");
    }

    @Test
    public void testSingleCatalogStalenessByTimestamp()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_timestamp_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO test_timestamp_base VALUES (1, 'test')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW iceberg.test_schema.test_timestamp_mv " +
                "WITH (" +
                "use_timestamp_based_staleness = true " +
                ") " +
                "AS SELECT * FROM test_timestamp_base");

        assertUpdate("REFRESH MATERIALIZED VIEW iceberg.test_schema.test_timestamp_mv", 1);

        assertQuery("SELECT * FROM test_timestamp_mv",
                "VALUES (1, 'test')");

        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);

        try {
            TableIdentifier viewId = TableIdentifier.of(Namespace.of("test_schema"), "test_timestamp_mv");
            View view = catalog.loadView(viewId);

            String useTimestampBased = view.properties().get(PRESTO_MATERIALIZED_VIEW_USE_TIMESTAMP_BASED_STALENESS);
            assertEquals(useTimestampBased, "true", "Expected use_timestamp_based_staleness to be true");

            String lastRefreshTimestamp = view.properties().get(PRESTO_MATERIALIZED_VIEW_LAST_REFRESH_TIMESTAMP);
            assertNotNull(lastRefreshTimestamp, "Expected last_refresh_timestamp to be set after refresh");
        }
        finally {
            catalog.close();
        }

        assertUpdate("DROP MATERIALIZED VIEW test_timestamp_mv");
        assertUpdate("DROP TABLE test_timestamp_base");
    }
}
