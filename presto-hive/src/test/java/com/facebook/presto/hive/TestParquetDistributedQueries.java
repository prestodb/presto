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
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveSessionProperties.COLLECT_COLUMN_STATISTICS_ON_WRITE;
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_ENABLED;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestParquetDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> parquetProperties = ImmutableMap.<String, String>builder()
                .put("hive.storage-format", "PARQUET")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.compression-codec", "GZIP")
                .put("hive.enable-parquet-dereference-pushdown", "true")
                .put("hive.partial_aggregation_pushdown_enabled", "true")
                .put("hive.partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true")
                .build();
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(
                getTables(),
                ImmutableMap.of(
                        "experimental.pushdown-subfields-enabled", "true",
                        "experimental.pushdown-dereference-enabled", "true"),
                "sql-standard",
                parquetProperties,
                Optional.empty());
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Test
    public void testQuickStats()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("hive." + COLLECT_COLUMN_STATISTICS_ON_WRITE, "false")
                .setSystemProperty("hive." + QUICK_STATS_ENABLED, "true")
                .build();

        getQueryRunner().execute(session, "CREATE TABLE test_quick_stats AS " +
                "SELECT orderkey, linenumber, shipdate," +
                " ARRAY[returnflag, linestatus] as arr," +
                " CAST(ROW(commitdate, receiptdate) AS ROW(date1 DATE,date2 DATE)) as rrow from lineitem");

        try {
            // Since no stats were collected during write, all column stats will be null
            assertQuery("SHOW STATS FOR test_quick_stats",
                    "SELECT * FROM (VALUES " +
                            "   ('orderkey', null, null, null, null, null, null, null), " +
                            "   ('linenumber', null, null, null, null, null, null, null), " +
                            "   ('shipdate', null, null, null, null, null, null, null), " +
                            "   ('arr', null, null, null, null, null, null, null), " +
                            "   ('rrow', null, null, null, null, null, null, null), " +
                            "   (null, null, null, null, 60175.0, null, null, null))");

            // With quick stats enabled, we should get nulls_fraction, low_value and high_value for the non-nested columns
            assertQuery(session, "SHOW STATS FOR test_quick_stats",
                    "SELECT * FROM (VALUES " +
                            "   ('orderkey', null, null, 0.0, null, '1', '60000', null), " +
                            "   ('linenumber', null, null, 0.0, null, '1', '7', null), " +
                            "   ('shipdate', null, null, 0.0, null, '1992-01-04', '1998-11-29', null), " +
                            "   ('arr', null, null, null, null, null, null, null), " +
                            "   ('rrow', null, null, null, null, null, null, null), " +
                            "   (null, null, null, null, 60175.0, null, null, null))");
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_quick_stats");
        }
    }

    @Test
    public void testQuickStatsPartitionedTable()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("hive." + COLLECT_COLUMN_STATISTICS_ON_WRITE, "false")
                .setSystemProperty("hive." + QUICK_STATS_ENABLED, "true")
                .build();

        getQueryRunner().execute(session, "CREATE TABLE test_quick_stats_partitioned (" +
                "    \"suppkey\" bigint," +
                "    \"linenumber\" integer," +
                "    \"orderkey\" bigint," +
                "    \"partkey\" bigint" +
                "   )" +
                "   WITH (" +
                "    format = 'PARQUET'," +
                "    partitioned_by = ARRAY['orderkey','partkey']" +
                "   )");

        getQueryRunner().execute(session, "INSERT INTO test_quick_stats_partitioned (suppkey, linenumber, orderkey, partkey)" +
                "VALUES" +
                "(1, 1, 100, 1000)," +
                "(2, 2, 101, 1001)," +
                "(3, 3, 102, 1002)," +
                "(4, 4, 103, 1003)," +
                "(5, 5, 104, 1004)," +
                "(6, 6, 105, 1005)," +
                "(7, 7, 106, 1006)," +
                "(8, 8, 107, 1007)," +
                "(9, 9, 108, 1008)," +
                "(10, 10, 109, 1009)");

        try {
            // Since no stats were collected during write, only the partitioned columns will have stats
            assertQuery("SHOW STATS FOR test_quick_stats_partitioned",
                    "SELECT * FROM (VALUES " +
                            "   ('suppkey', null, null, null, null, null, null, null), " +
                            "   ('linenumber', null, null, null, null, null, null, null), " +
                            "   ('orderkey', null, 10.0, 0.0, null, 100, 109, null), " +
                            "   ('partkey', null, 10.0, 0.0, null, 1000, 1009, null), " +
                            "   (null, null, null, null, 10.0, null, null, null))");

            // With quick stats enabled, we should get nulls_fraction, low_value and high_value for all columns
            assertQuery(session, "SHOW STATS FOR test_quick_stats_partitioned",
                    "SELECT * FROM (VALUES " +
                            "   ('suppkey', null, null, 0.0, null, 1, 10, null), " +
                            "   ('linenumber', null, null, 0.0, null, 1, 10, null), " +
                            "   ('orderkey', null, 10.0, 0.0, null, 100, 109, null), " +
                            "   ('partkey', null, 10.0, 0.0, null, 1000, 1009, null), " +
                            "   (null, null, null, null, 10.0, null, null, null))");

            // If a query targets a specific partition, stats are correctly limited to that partition
            assertQuery(session, "show stats for (select * from test_quick_stats_partitioned where partkey = 1009)",
                    "SELECT * FROM (VALUES " +
                            "   ('suppkey', null, null, 0.0, null, 10, 10, null), " +
                            "   ('linenumber', null, null, 0.0, null, 10, 10, null), " +
                            "   ('orderkey', null, 1.0, 0.0, null, 109, 109, null), " +
                            "   ('partkey', null, 1.0, 0.0, null, 1009, 1009, null), " +
                            "   (null, null, null, null, 1.0, null, null, null))");
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_quick_stats_partitioned");
        }
    }

    @Test
    public void testSubfieldPruning()
    {
        getQueryRunner().execute("CREATE TABLE test_subfield_pruning AS " +
                "SELECT orderkey, linenumber, shipdate, " +
                "   CAST(ROW(orderkey, linenumber, ROW(day(shipdate), month(shipdate), year(shipdate))) " +
                "       AS ROW(orderkey BIGINT, linenumber INTEGER, shipdate ROW(ship_day TINYINT, ship_month TINYINT, ship_year INTEGER))) AS info " +
                "FROM lineitem");

        try {
            assertQuery("SELECT info.orderkey, info.shipdate.ship_month FROM test_subfield_pruning", "SELECT orderkey, month(shipdate) FROM lineitem");

            assertQuery("SELECT orderkey FROM test_subfield_pruning WHERE info.shipdate.ship_month % 2 = 0", "SELECT orderkey FROM lineitem WHERE month(shipdate) % 2 = 0");
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_subfield_pruning");
        }
    }

    @Test
    public void testSubfieldWithSchemaChanges()
    {
        getQueryRunner().execute("CREATE TABLE test_subfield_multilevel_pruning AS " +
                "SELECT " +
                "   1 as orderkey," +
                "   CAST(ROW('N', ROW(5, 7)) AS ROW(returnflag CHAR(1), shipdate ROW(ship_day INTEGER, ship_month INTEGER))) as nestedColumnLevelUpdates," +
                "   CAST(ROW('N', ROW(5, 7)) AS ROW(returnflag CHAR(1), shipdate ROW(ship_day INTEGER, ship_month INTEGER))) as colLevelUpdates");

        // update the schema of the nested column
        getQueryRunner().execute("ALTER TABLE test_subfield_multilevel_pruning DROP COLUMN nestedColumnLevelUpdates");
        getQueryRunner().execute("ALTER TABLE test_subfield_multilevel_pruning ADD COLUMN " +
                "nestedColumnLevelUpdates ROW(returnflag CHAR(1), shipdate ROW(ship_day INTEGER, ship_month INTEGER, ship_year INTEGER))");

        // delete the entire struct column (colLevelUpdates) and add a new struct column with different name (colLevelUpdates2)
        getQueryRunner().execute("ALTER TABLE test_subfield_multilevel_pruning DROP COLUMN colLevelUpdates");
        getQueryRunner().execute("ALTER TABLE test_subfield_multilevel_pruning ADD COLUMN " +
                "colLevelUpdates2 ROW(returnflag CHAR(1), shipdate ROW(ship_day INTEGER, ship_month INTEGER, ship_year INTEGER))");

        getQueryRunner().execute("INSERT INTO test_subfield_multilevel_pruning " +
                "SELECT 2, cast(row('Y', ROW(5, 7, 2020)) as ROW(returnflag CHAR(1), shipdate ROW(ship_day INTEGER, ship_month INTEGER, ship_year INTEGER)))," +
                "cast(row('Y', ROW(5, 7, 2020)) as ROW(returnflag CHAR(1), shipdate ROW(ship_day INTEGER, ship_month INTEGER, ship_year INTEGER)))");

        try {
            assertQuery("SELECT orderkey, nestedColumnLevelUpdates.shipdate.ship_day, nestedColumnLevelUpdates.shipdate.ship_month, nestedColumnLevelUpdates.shipdate.ship_year FROM test_subfield_multilevel_pruning",
                    "SELECT * from (VALUES(1, 5, 7, null), (2, 5, 7, 2020))");

            assertQuery("SELECT orderkey, colLevelUpdates2.shipdate.ship_day, colLevelUpdates2.shipdate.ship_month, colLevelUpdates2.shipdate.ship_year FROM test_subfield_multilevel_pruning",
                    "SELECT * from (VALUES (1, null, null, null), (2, 5, 7, 2020))");
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_subfield_multilevel_pruning");
        }
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
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

    @Override
    public void testRenameColumn()
    {
        // Parquet field lookup use column name does not support Rename
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN ", query, LOGICAL));
    }
}
