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
package com.facebook.presto.sidecar;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;

public class TestNativeSidecarQueriesOnSystemTables
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createNation(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createRegion(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .build();
        TestNativeSidecarPlugin.setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testExtractSystemTableFilterCorrectness()
    {
        // FilterScanRule - Basic filter with CPP function on system table
        assertQuery("SELECT table_name, ordinal_position FROM information_schema.columns " +
                "WHERE abs(ordinal_position) > 0 AND table_catalog = 'hive' AND table_name != 'roles' " +
                "ORDER BY table_name, ordinal_position");

        // FilterScanRule - Complex predicate with multiple CPP functions
        assertQuery("SELECT table_name, ordinal_position FROM information_schema.columns " +
                "WHERE (abs(ordinal_position) > 1 AND ordinal_position < 5) " +
                "OR (abs(ordinal_position) + abs(ordinal_position) = 2 * ordinal_position) " +
                "AND table_catalog = 'hive' AND table_name != 'roles'" +
                "ORDER BY table_name, ordinal_position");

        // ProjectScanRule - CPP function in projection
        assertQuery("SELECT table_name, abs(ordinal_position) as abs_pos FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_name IN ('nation', 'region', 'lineitem', 'orders') " +
                "ORDER BY table_name, abs_pos");

        // FilterScanRule with output variable mismatch
        assertQuery("SELECT table_name " +
                "FROM information_schema.columns " +
                "WHERE abs(ordinal_position) > 2 " +
                "AND table_catalog = 'hive' AND table_name IN ('nation', 'region', 'lineitem', 'orders') " +
                "ORDER BY table_name");

        // ProjectFilterScanRule - Project with CPP and Filter on system table
        assertQuery("SELECT table_name, abs(ordinal_position) as abs_pos " +
                "FROM information_schema.columns " +
                "WHERE ordinal_position > 0 AND abs(ordinal_position) < 10 " +
                "AND table_catalog = 'hive' AND table_name IN ('nation', 'region', 'lineitem', 'orders') " +
                "ORDER BY table_name, abs_pos");

        // Join system table with regular table using CPP function
        assertQuery("SELECT c.table_name, c.ordinal_position, n.name " +
                "FROM information_schema.columns c " +
                "JOIN nation n ON abs(c.ordinal_position) = n.nationkey " +
                "WHERE c.table_catalog = 'hive' AND c.table_name IN ('nation', 'region', 'lineitem', 'orders') " +
                "ORDER BY c.table_name, c.ordinal_position");

        // Aggregation with CPP function on system table
        assertQuery("SELECT table_name, COUNT(*), SUM(abs(ordinal_position)) " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_name IN ('nation', 'region', 'lineitem', 'orders') " +
                "GROUP BY table_name " +
                "ORDER BY table_name");

        // Nested CPP functions
        assertQuery("SELECT table_name, ordinal_position " +
                "FROM information_schema.columns " +
                "WHERE abs(abs(ordinal_position)) = ordinal_position " +
                "AND table_catalog = 'hive' AND table_name IN ('nation', 'region') " +
                "ORDER BY table_name, ordinal_position");

        // CPP function in IN predicate
        assertQuery("SELECT table_name, ordinal_position " +
                "FROM information_schema.columns " +
                "WHERE abs(ordinal_position) IN (1, 2, 3) " +
                "AND table_catalog = 'hive' AND table_name != 'roles' " +
                "ORDER BY table_name, ordinal_position");

        // CPP function with NULL handling
        assertQuery("SELECT table_name, " +
                "COALESCE(abs(ordinal_position), 0) as abs_pos " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_name IN ('nation', 'region') " +
                "ORDER BY table_name, ordinal_position");
    }

    @Test
    public void testExtractSystemTableFilterWithJoins()
    {
        // Self-join on system table with CPP function
        assertQuery("SELECT c1.table_name, c1.ordinal_position, c2.ordinal_position " +
                "FROM information_schema.columns c1 " +
                "JOIN information_schema.columns c2 " +
                "ON c1.table_name = c2.table_name " +
                "AND abs(c1.ordinal_position) = abs(c2.ordinal_position) " +
                "WHERE c1.table_catalog = 'hive' AND c2.table_catalog = 'hive' " +
                "AND c1.table_name = 'nation' " +
                "ORDER BY c1.table_name, c1.ordinal_position, c2.ordinal_position");

        // Join with CPP function in join condition
        assertQuery("SELECT c.table_name, c.column_name, t.table_type " +
                "FROM information_schema.columns c " +
                "JOIN information_schema.tables t " +
                "ON c.table_schema = t.table_schema " +
                "AND c.table_name = t.table_name " +
                "WHERE abs(c.ordinal_position) <= 3 " +
                "AND c.table_catalog = 'hive' " +
                "AND t.table_catalog = 'hive' " +
                "AND c.table_name IN ('nation', 'region') " +
                "ORDER BY c.table_name, c.column_name");

        // Join system table with aggregation using CPP function
        assertQuery("SELECT t.table_name, COUNT(c.column_name), MAX(abs(c.ordinal_position)) " +
                "FROM information_schema.tables t " +
                "JOIN information_schema.columns c " +
                "ON t.table_schema = c.table_schema AND t.table_name = c.table_name " +
                "WHERE t.table_catalog = 'hive' AND c.table_catalog = 'hive' " +
                "AND t.table_name IN ('nation', 'region') " +
                "GROUP BY t.table_name " +
                "ORDER BY t.table_name");

        // Complex join with multiple CPP functions
        assertQuery("SELECT c1.table_name, COUNT(DISTINCT c2.column_name) " +
                "FROM information_schema.columns c1 " +
                "JOIN information_schema.columns c2 " +
                "ON c1.table_schema = c2.table_schema " +
                "WHERE abs(c1.ordinal_position) + abs(c2.ordinal_position) > 3 " +
                "AND c1.table_catalog = 'hive' AND c2.table_catalog = 'hive' " +
                "AND c1.table_name IN ('nation', 'region') " +
                "GROUP BY c1.table_name " +
                "ORDER BY c1.table_name");

        // Left join with CPP function
        assertQuery("SELECT t.table_name, c.column_name, abs(c.ordinal_position) " +
                "FROM information_schema.tables t " +
                "LEFT JOIN information_schema.columns c " +
                "ON t.table_schema = c.table_schema " +
                "AND t.table_name = c.table_name " +
                "AND abs(c.ordinal_position) <= 2 " +
                "WHERE t.table_catalog = 'hive' " +
                "AND t.table_name IN ('nation', 'region') " +
                "ORDER BY t.table_name, c.ordinal_position");
    }

    @Test
    public void testExtractSystemTableFilterWithSubqueries()
    {
        // CPP function in subquery
        assertQuery("SELECT table_name FROM information_schema.tables " +
                "WHERE table_catalog = 'hive' " +
                "AND table_name IN (" +
                "  SELECT table_name FROM information_schema.columns " +
                "  WHERE abs(ordinal_position) = 1 " +
                "  AND table_catalog = 'hive'  AND table_name != 'roles'" +
                ") " +
                "ORDER BY table_name");

        // Correlated subquery with CPP function
        assertQuery("SELECT DISTINCT t.table_name " +
                "FROM information_schema.tables t " +
                "WHERE t.table_catalog = 'hive' " +
                "AND EXISTS (" +
                "  SELECT 1 FROM information_schema.columns c " +
                "  WHERE c.table_name = t.table_name " +
                "  AND c.table_catalog = t.table_catalog " +
                "  AND abs(c.ordinal_position) > 2" +
                ") " +
                "AND t.table_name IN ('nation', 'region', 'lineitem', 'orders') " +
                "ORDER BY t.table_name");

        // Scalar subquery with CPP function
        assertQuery("SELECT table_name, " +
                "(SELECT COUNT(*) FROM information_schema.columns c2 " +
                " WHERE c2.table_name = c1.table_name " +
                " AND c2.table_catalog = c1.table_catalog " +
                " AND abs(c2.ordinal_position) <= 3) as col_count " +
                "FROM information_schema.columns c1 " +
                "WHERE c1.table_catalog = 'hive' " +
                "AND c1.table_name IN ('nation', 'region') " +
                "AND c1.ordinal_position = 1 " +
                "ORDER BY c1.table_name");
    }

    @Test
    public void testExtractSystemTableFilterWithWindowFunctions()
    {
        // Window function with CPP function in partition
        assertQuery("SELECT table_name, ordinal_position, " +
                "row_number() OVER (PARTITION BY table_name ORDER BY abs(ordinal_position)) as rn " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' " +
                "AND table_name IN ('nation', 'region') " +
                "ORDER BY table_name, ordinal_position");

        // Window function with CPP function filter
        assertQuery("SELECT * FROM (" +
                "  SELECT table_name, ordinal_position, " +
                "  row_number() OVER (PARTITION BY table_name ORDER BY ordinal_position) as rn " +
                "  FROM information_schema.columns " +
                "  WHERE table_catalog = 'hive' " +
                "  AND table_name IN ('nation', 'region')" +
                ") " +
                "WHERE abs(rn) <= 2 " +
                "ORDER BY table_name, ordinal_position");
    }

    @Test
    public void testExtractSystemTableFilterWithSetOperations()
    {
        // UNION with CPP functions
        assertQuery("SELECT table_name, abs(ordinal_position) as pos " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_name = 'nation' " +
                "UNION ALL " +
                "SELECT table_name, abs(ordinal_position) as pos " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_name = 'region' " +
                "ORDER BY table_name, pos");

        // INTERSECT with CPP functions
        assertQuery("SELECT abs(ordinal_position) as pos " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_name = 'nation' " +
                "INTERSECT " +
                "SELECT abs(ordinal_position) as pos " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_name = 'region' " +
                "ORDER BY pos");
    }
}
