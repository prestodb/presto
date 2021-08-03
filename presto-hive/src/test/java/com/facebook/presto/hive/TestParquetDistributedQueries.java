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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;

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
        return HiveQueryRunner.createQueryRunner(
                getTables(),
                ImmutableMap.of(
                        "experimental.pushdown-subfields-enabled", "true",
                        "experimental.pushdown-dereference-enabled", "true"),
                "sql-standard",
                parquetProperties,
                Optional.empty());
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

    @Override
    public void testInsertWithCoercion()
    {
        assertUpdate("CREATE TABLE test_insert_with_coercion (" +
                "tinyint_column TINYINT, " +
                "integer_column INTEGER, " +
                "decimal_column DECIMAL(5, 3), " +
                "real_column REAL, " +
                "char_column CHAR(3), " +
                "bounded_varchar_column VARCHAR(3), " +
                "unbounded_varchar_column VARCHAR, " +
                "date_column DATE)");

        assertUpdate("INSERT INTO test_insert_with_coercion (tinyint_column, integer_column, decimal_column, real_column) VALUES (1e0, 2e0, 3e0, 4e0)", 1);
        assertUpdate("INSERT INTO test_insert_with_coercion (char_column, bounded_varchar_column, unbounded_varchar_column) VALUES (CAST('aa     ' AS varchar), CAST('aa     ' AS varchar), CAST('aa     ' AS varchar))", 1);
        assertUpdate("INSERT INTO test_insert_with_coercion (date_column) VALUES (TIMESTAMP '2019-11-18 22:13:40')", 1);

        assertQuery(
                "SELECT * FROM test_insert_with_coercion",
                "VALUES " +
                        "(1, 2, 3, 4, NULL, NULL, NULL, NULL), " +
                        "(NULL, NULL, NULL, NULL, 'aa ', 'aa ', 'aa     ', NULL), " +
                        "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, DATE '2019-11-18')");

        assertQueryFails("INSERT INTO test_insert_with_coercion (integer_column) VALUES (3e9)", "Out of range for integer: 3.0E9");
        /* Parquet silently truncates non-space characters when inserting, the following tests are N/A */
        //assertQueryFails("INSERT INTO test_insert_with_coercion (char_column) VALUES ('abcd')", "Cannot truncate non-space characters on INSERT");
        //assertQueryFails("INSERT INTO test_insert_with_coercion (bounded_varchar_column) VALUES ('abcd')", "Cannot truncate non-space characters on INSERT");

        assertUpdate("DROP TABLE test_insert_with_coercion");
    }
}
