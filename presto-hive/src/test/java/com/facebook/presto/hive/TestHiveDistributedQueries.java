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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.hive.TestHiveEventListenerPlugin.TestingHiveEventListener;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.CTE_PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_FOR_MAP_FUNCTIONS;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_FROM_LAMBDA_ENABLED;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_OPTIMIZER_INFO_ENABLED;
import static com.facebook.presto.hive.HiveCommonSessionProperties.ORC_USE_COLUMN_NAMES;
import static com.facebook.presto.hive.HiveTestUtils.getHiveTableProperty;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.tpch.TpchTable.getTables;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(getTables());
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        assertFalse(getQueryRunner().getEventListeners().isEmpty());
        EventListener eventListener = getQueryRunner().getEventListeners().get(0);
        assertTrue(eventListener instanceof TestingHiveEventListener, eventListener.getClass().getName());
        Set<QueryId> runningQueryIds = ((TestingHiveEventListener) eventListener).getRunningQueries();

        if (!runningQueryIds.isEmpty()) {
            // Await query events to propagate and finish
            Thread.sleep(1000);
        }
        assertEquals(
                runningQueryIds,
                ImmutableSet.of(),
                format(
                        "Query completion events not sent for %d queries: %s",
                        runningQueryIds.size(),
                        runningQueryIds.stream().map(QueryId::getId).collect(joining(", "))));
        super.close();
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN ", query, LOGICAL));
    }

    @Test
    public void testTrackMaterializedCTEs()
    {
        Session materializedSession = Session.builder(getSession())
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .setSystemProperty(CTE_PARTITIONING_PROVIDER_CATALOG, "hive")
                .build();

        String query = "with tbl as (select * from lineitem), tbl2 as (select * from tbl) select * from tbl, tbl2";
        MaterializedResult materializedResult = computeActual(materializedSession, "explain " + query);
        String explain = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        checkCTEInfo(explain, "tbl", 2, false, true);
        checkCTEInfo(explain, "tbl2", 1, false, true);
    }

    @Test
    public void testPushdownSubfieldForMapFunctionsInLambda()
    {
        Session enabled = Session.builder(getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_FOR_MAP_FUNCTIONS, "true")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_FROM_LAMBDA_ENABLED, "true")
                .build();
        Session disabled = Session.builder(getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_FOR_MAP_FUNCTIONS, "false")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "false")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_FROM_LAMBDA_ENABLED, "false")
                .build();
        try {
            getQueryRunner().execute(
                    "CREATE TABLE test_pushdown_subfields AS\n" +
                            "SELECT * FROM (\n" +
                            "  VALUES \n" +
                            "    (3, '2025-01-08', \n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY[-2, 1], ARRAY[0.34, 0.92]),\n" +
                            "        MAP(ARRAY[3, 4], ARRAY[0.12, 0.88])\n" +
                            "      ],\n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY['a', 'b'], ARRAY[0.56, 0.44]),\n" +
                            "        MAP(ARRAY['c', 'd'], ARRAY[0.90, 0.10])\n" +
                            "      ]\n" +
                            "    ),\n" +
                            "    (1, '2025-01-02', \n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY[1, 2], ARRAY[0.23, 0.45]),\n" +
                            "        MAP(ARRAY[5, 6], ARRAY[0.67, 0.89])\n" +
                            "      ],\n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY['x', 'y'], ARRAY[0.78, 0.22]),\n" +
                            "        MAP(ARRAY['z', 'w'], ARRAY[0.11, 0.99])\n" +
                            "      ]\n" +
                            "    ),\n" +
                            "    (7, '2025-01-17', \n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY[-1, 0], ARRAY[0.60, 0.70]),\n" +
                            "        MAP(ARRAY[2, 3], ARRAY[0.21, 0.79])\n" +
                            "      ],\n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY['m', 'n'], ARRAY[0.43, 0.57]),\n" +
                            "        MAP(ARRAY['o', 'p'], ARRAY[0.25, 0.75])\n" +
                            "      ]\n" +
                            "    ),\n" +
                            "    (2, '2025-01-06', \n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY[4, 5], ARRAY[0.75, 0.32]),\n" +
                            "        MAP(ARRAY[6, 7], ARRAY[0.19, 0.46])\n" +
                            "      ],\n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY['q', 'r'], ARRAY[0.98, 0.02]),\n" +
                            "        MAP(ARRAY['s', 't'], ARRAY[0.49, 0.51])\n" +
                            "      ]\n" +
                            "    ),\n" +
                            "    (5, '2025-01-14', \n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY[8, 9], ARRAY[0.88, 0.99]),\n" +
                            "        MAP(ARRAY[10, 11], ARRAY[0.00, 0.33])\n" +
                            "      ],\n" +
                            "      ARRAY[\n" +
                            "        MAP(ARRAY['u', 'v'], ARRAY[0.66, 0.34]),\n" +
                            "        MAP(ARRAY['w', 'x'], ARRAY[0.17, 0.83])\n" +
                            "      ]\n" +
                            "    )\n" +
                            ") t(id, ds, array_of_maps_int, array_of_maps_str)");

            @Language("SQL") String sql = "select transform(array_of_maps_int, item -> map_filter(item, (k, v) -> k = 1)), " +
                    "transform(array_of_maps_str, item -> map_filter(item, (k, v) -> k = 'x')) from test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "SELECT \n" +
                    "  transform(array_of_maps_int, item -> map_filter(item, (k, v) -> contains(array[-2, 1, 0], k))),\n" +
                    "  transform(array_of_maps_str, item -> map_filter(item, (k, v) -> contains(array['a', 'x'], k)))\n" +
                    "FROM test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "SELECT \n" +
                    "  transform(array_of_maps_int, item -> map_subset(item, array[-2, 1, 0])),\n" +
                    "  transform(array_of_maps_str, item -> map_subset(item, array['a', 'x']))\n" +
                    "FROM test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "SELECT \n" +
                    "  transform(array_of_maps_int, item -> map_filter(item, (k, v) -> k in (-2, 1, 0))),\n" +
                    "  transform(array_of_maps_str, item -> map_filter(item, (k, v) -> k in ('a', 'x')))\n" +
                    "FROM test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "SELECT \n" +
                    "  transform(array_of_maps_int, item -> map_filter(item, (k, v) -> k = 1)),\n" +
                    "  transform(array_of_maps_str, item -> map_filter(item, (k, v) -> k = 'a'))\n" +
                    "FROM test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "SELECT \n" +
                    "  transform(array_of_maps_int, item -> map_filter(item, (k, v) -> contains(array[-2, 1, id], k))),\n" +
                    "  transform(array_of_maps_str, item -> map_filter(item, (k, v) -> contains(array['a', 'x'], k)))\n" +
                    "FROM test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "SELECT \n" +
                    "  transform(array_of_maps_int, item -> map_subset(item, array[-2, 1, id])),\n" +
                    "  transform(array_of_maps_str, item -> map_subset(item, array['a', 'x']))\n" +
                    "FROM test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "SELECT \n" +
                    "  transform(array_of_maps_int, item -> map_filter(item, (k, v) -> k in (-2, 1, null))),\n" +
                    "  transform(array_of_maps_str, item -> map_filter(item, (k, v) -> k in ('a', 'x')))\n" +
                    "FROM test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "SELECT \n" +
                    "  transform(array_of_maps_int, item -> map_filter(item, (k, v) -> k = id)),\n" +
                    "  transform(array_of_maps_str, item -> map_filter(item, (k, v) -> k = 'a'))\n" +
                    "FROM test_pushdown_subfields";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_pushdown_subfields");
        }
    }

    @Test
    public void testPushdownSubfieldForMapFunctions()
    {
        Session enabled = Session.builder(getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_FOR_MAP_FUNCTIONS, "true")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .build();
        Session disabled = Session.builder(getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_FOR_MAP_FUNCTIONS, "false")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "false")
                .build();
        try {
            getQueryRunner().execute(
                    "CREATE TABLE test_pushdown_subfields_map_functions AS\n" +
                            "SELECT * FROM (\n" +
                            "  VALUES \n" +
                            "    (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92]), MAP(ARRAY['a', 'b'], ARRAY[0.12, 0.88])),\n" +
                            "    (1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5]), MAP(ARRAY['x', 'y'], ARRAY[0.45, 0.55])),\n" +
                            "    (7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70]), MAP(ARRAY['m', 'n'], ARRAY[0.21, 0.79])),\n" +
                            "    (2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46]), MAP(ARRAY['p', 'q', 'r'], ARRAY[0.11, 0.22, 0.67])),\n" +
                            "    (5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00]), MAP(ARRAY['s', 't', 'u'], ARRAY[0.33, 0.44, 0.23])),\n" +
                            "    (4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55]), MAP(ARRAY['v', 'w'], ARRAY[0.66, 0.34])),\n" +
                            "    (8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55]), MAP(ARRAY['i', 'j', 'k'], ARRAY[0.78, 0.89, 0.12])),\n" +
                            "    (6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50]), MAP(ARRAY['c', 'd'], ARRAY[0.90, 0.10])),\n" +
                            "    (2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21]), MAP(ARRAY['e', 'f'], ARRAY[0.56, 0.44])),\n" +
                            "    (1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67]), MAP(ARRAY['g', 'h'], ARRAY[0.23, 0.77]))\n" +
                            ") AS t(id, ds, feature, extra_feature)");

            @Language("SQL") String sql = "select map_filter(feature, (k, v) -> k in (-2, 1, 0)), map_filter(extra_feature, (k, v) -> k in ('a', 'x')) from test_pushdown_subfields_map_functions";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "select map_filter(feature, (k, v) -> contains(array[-2, 1, 0], k)), map_filter(extra_feature, (k, v) -> contains(array['a', 'x'], k)) from test_pushdown_subfields_map_functions";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "select map_filter(feature, (k, v) -> k = 0), map_filter(extra_feature, (k, v) -> k = 'a') from test_pushdown_subfields_map_functions";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "select map_subset(feature, array[-2, 1, 0]), map_subset(extra_feature, array['a', 'x']) from test_pushdown_subfields_map_functions";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);

            sql = "select map_filter(feature, (k, v) -> k in (-2, 1, id)), map_filter(extra_feature, (k, v) -> k in ('a', 'x')) from test_pushdown_subfields_map_functions";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "select map_filter(feature, (k, v) -> contains(array[-2, 1, id], k)), map_filter(extra_feature, (k, v) -> contains(array['a', 'x', null], k)) from test_pushdown_subfields_map_functions";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "select map_filter(feature, (k, v) -> k = id), map_filter(extra_feature, (k, v) -> k = 'a') from test_pushdown_subfields_map_functions";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
            sql = "select map_subset(feature, array[id]), map_subset(extra_feature, array['a', null]) from test_pushdown_subfields_map_functions";
            assertQueryWithSameQueryRunner(enabled, sql, disabled);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_pushdown_subfields_map_functions");
        }
    }

    @Test
    public void testOrcUseColumnNames() throws IOException, URISyntaxException
    {
        File externalTableDataDirectory = Files.createTempDir();
        String externalTableDataLocationUri = externalTableDataDirectory.toURI().toString();

        try {
            @Language("SQL") String createManagedTableSql = "" +
                            "create table test_orc_use_column_names (\n" +
                            "   \"c1\" int,\n" +
                            "   \"c2\" varchar\n" +
                            ")\n" +
                            "WITH (\n" +
                            "   format = 'ORC'\n" +
                            ")";

            assertUpdate(createManagedTableSql);
            assertUpdate(format("insert into test_orc_use_column_names values (1, 'one')"), 1);
            String tablePath = (String) getHiveTableProperty(getQueryRunner(), getSession(), "test_orc_use_column_names", (HiveTableLayoutHandle table) -> table.getTablePath());
            File managedTableDataDirectory = new File(new URI(tablePath).getRawPath());

            assertTrue(managedTableDataDirectory.isDirectory(), "Source managed table data directory does not exist: " + managedTableDataDirectory);
            File[] orcFiles = managedTableDataDirectory.listFiles(file -> file.isFile()
                    && !file.getName().contains("."));

            if (orcFiles != null) {
                for (File orcFile : orcFiles) {
                    File destinationFile = new File(externalTableDataDirectory, orcFile.getName());
                    Files.copy(orcFile, destinationFile);
                }
            }
            else {
                throw new IllegalStateException("No ORC files found in managed table data directory: " + managedTableDataDirectory);
            }

            @Language("SQL") String createMisMatchingExternalTableSql = format("" +
                            "CREATE TABLE test_orc_use_column_names_mismatching_ext (\n" +
                            "   \"c2\" varchar,\n" +
                            "   \"c1\" int\n" +
                            ")\n" +
                            "WITH (\n" +
                            "   external_location = '%s',\n" +
                            "   format = 'ORC'\n" +
                            ")",
                    externalTableDataLocationUri);
            assertUpdate(createMisMatchingExternalTableSql);

            assertQueryFails(getSession(),
                    "select * from test_orc_use_column_names_mismatching_ext",
                    ".*java.io.IOException: Malformed ORC file. Can not read SQL type varchar from ORC stream .c1 of type INT.*");

            Session useOrcColumnNamesSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty("hive", ORC_USE_COLUMN_NAMES, "true").build();
            assertQuerySucceeds(useOrcColumnNamesSession, "select * from test_orc_use_column_names_mismatching_ext");

            @Language("SQL") String createDifferentColumnNameExternalTableSql = format("" +
                    "CREATE TABLE test_orc_use_column_names_different_column_name_ext (\n" +
                    "   \"c1\" int,\n" +
                    "   \"c3\" varchar\n" +
                    ")\n" +
                    "WITH (\n" +
                    "   external_location = '%s',\n" +
                    "   format = 'ORC'\n" +
                    ")",
                    externalTableDataLocationUri);
            assertUpdate(createDifferentColumnNameExternalTableSql);

            assertQuery(useOrcColumnNamesSession, "select * from test_orc_use_column_names_different_column_name_ext", "VALUES (1, NULL)");

            @Language("SQL") String createAdditionalColumnExternalTableSql = format("" +
                    "CREATE TABLE test_orc_use_column_names_additional_column_ext (\n" +
                    "   \"c1\" int,\n" +
                    "   \"c2\" varchar,\n" +
                    "   \"c3\" varchar\n" +
                    ")\n" +
                    "WITH (\n" +
                    "   external_location = '%s',\n" +
                    "   format = 'ORC'\n" +
                    ")",
                    externalTableDataLocationUri);
            assertUpdate(createAdditionalColumnExternalTableSql);

            assertQuery(useOrcColumnNamesSession, "select * from test_orc_use_column_names_additional_column_ext", "VALUES (1, 'one', NULL)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_orc_use_column_names");
            assertUpdate("DROP TABLE IF EXISTS test_orc_use_column_names_mismatching_ext");
            assertUpdate("DROP TABLE IF EXISTS test_orc_use_column_names_different_column_name_ext");
            assertUpdate("DROP TABLE IF EXISTS test_orc_use_column_names_additional_column_ext");

            if (externalTableDataDirectory != null) {
                deleteRecursively(externalTableDataDirectory.toPath(), ALLOW_INSECURE);
            }
        }
    }

    // Hive specific tests should normally go in TestHiveIntegrationSmokeTest
}
