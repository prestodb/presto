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
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

/**
 * Plan-level tests for MaterializedView optimizer rule.
 * Verifies that the optimizer correctly decides when to use UNION stitching vs full recompute.
 */
@Test(singleThreaded = true)
public class TestIcebergMaterializedViewOptimizer
        extends AbstractTestQueryFramework
{
    private File warehouseLocation;
    private TestingHttpServer restServer;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(restServer.getBaseUrl().toString()))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .setExtraProperties(ImmutableMap.of("experimental.legacy-materialized-views", "false"))
                .build().getQueryRunner();
    }

    @Test
    public void testBasicOptimization()
    {
        assertUpdate("CREATE TABLE base_no_parts (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO base_no_parts VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_no_parts AS SELECT id, value FROM base_no_parts");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_no_parts");

        assertUpdate("INSERT INTO base_no_parts VALUES (3, 300)", 1);

        assertPlan("SELECT * FROM mv_no_parts",
                anyTree(tableScan("base_no_parts")));

        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_no_parts");

        assertPlan("SELECT * FROM mv_no_parts",
                anyTree(tableScan("__mv_storage__mv_no_parts")));

        assertUpdate("DROP MATERIALIZED VIEW mv_no_parts");
        assertUpdate("DROP TABLE base_no_parts");
    }

    @Test
    public void testMultiTableStaleness()
    {
        // Create two partitioned base tables
        assertUpdate("CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, ds VARCHAR) " +
                "WITH (partitioning = ARRAY['ds'])");
        assertUpdate("CREATE TABLE customers (customer_id BIGINT, name VARCHAR, reg_date VARCHAR) " +
                "WITH (partitioning = ARRAY['reg_date'])");

        assertUpdate("INSERT INTO orders VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO customers VALUES (100, 'Alice', '2024-01-01')", 1);

        // Create JOIN MV with partition columns in output
        assertUpdate("CREATE MATERIALIZED VIEW mv_join AS " +
                "SELECT o.order_id, c.name, o.ds, c.reg_date " +
                "FROM orders o JOIN customers c ON o.customer_id = c.customer_id");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_join");

        // Make one table stale
        assertUpdate("INSERT INTO orders VALUES (2, 200, '2024-01-02')", 1);

        assertPlan("SELECT * FROM mv_join",
                anyTree(
                        anyTree(
                                join(
                                        anyTree(tableScan("orders")),
                                        anyTree(tableScan("customers"))))));

        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_join");

        // Make both tables stale
        assertUpdate("INSERT INTO orders VALUES (2, 200, '2024-01-02')", 1);
        assertUpdate("INSERT INTO customers VALUES (200, 'Bob', '2024-01-02')", 1);

        assertPlan("SELECT * FROM mv_join",
                anyTree(
                        anyTree(
                                join(
                                        anyTree(tableScan("orders")),
                                        anyTree(tableScan("customers"))))));
        assertUpdate("DROP MATERIALIZED VIEW mv_join");
        assertUpdate("DROP TABLE customers");
        assertUpdate("DROP TABLE orders");
    }
}
