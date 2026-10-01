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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOracleViews
        extends AbstractTestQueryFramework
{
    private static OracleServerTester oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new OracleServerTester();
        return createOracleQueryRunner(oracleServer, TpchTable.ORDERS);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    @Test
    public void testCreateView()
    {
        // Create a simple view
        assertUpdate("CREATE VIEW test_view AS SELECT orderkey, custkey FROM orders");

        // Query the view
        MaterializedResult result = computeActual("SELECT orderkey FROM test_view WHERE orderkey = 1");
        assertEquals(result.getRowCount(), 1);

        // Cleanup
        assertUpdate("DROP VIEW test_view");
    }

    @Test
    public void testShowCreateView()
    {
        // Create a view
        assertUpdate("CREATE VIEW test_show_view AS SELECT orderkey, totalprice FROM orders");

        // Show create view
        MaterializedResult result = computeActual("SHOW CREATE VIEW test_show_view");
        assertEquals(result.getRowCount(), 1);
        String viewDefinition = (String) result.getOnlyValue();
        assertTrue(viewDefinition.contains("CREATE VIEW"), "View definition should contain CREATE VIEW");
        assertTrue(viewDefinition.contains("orderkey"), "View definition should contain orderkey column");
        assertTrue(viewDefinition.contains("totalprice"), "View definition should contain totalprice column");

        // Cleanup
        assertUpdate("DROP VIEW test_show_view");
    }

    @Test
    public void testListViewsWithShowTables()
    {
        String[] viewNames = {"view1", "view2"};
        for (String viewName : viewNames) {
            assertUpdate("CREATE VIEW " + viewName + " AS SELECT orderkey FROM orders");
        }

        MaterializedResult result = computeActual("SHOW TABLES");
        List<Object> tableNames = new ArrayList<>(result.getOnlyColumnAsSet());

        for (String viewName : viewNames) {
            boolean found = tableNames.stream()
                    .anyMatch(name -> viewName.equalsIgnoreCase((String) name));
            assertTrue(found, viewName + " should be listed in SHOW TABLES");
        }

        for (String viewName : viewNames) {
            assertUpdate("DROP VIEW " + viewName);
        }
    }

    @Test
    public void testDropView()
    {
        assertUpdate("CREATE VIEW test_drop_view AS SELECT orderkey FROM orders");

        MaterializedResult result = computeActual("SHOW TABLES");
        List<Object> tableNames = new ArrayList<>(result.getOnlyColumnAsSet());
        boolean viewExists = tableNames.stream()
                .anyMatch(name -> "test_drop_view".equalsIgnoreCase((String) name));
        assertTrue(viewExists, "View should exist after creation");

        assertUpdate("DROP VIEW test_drop_view");

        result = computeActual("SHOW TABLES");
        tableNames = new ArrayList<>(result.getOnlyColumnAsSet());
        viewExists = tableNames.stream()
                .anyMatch(name -> "test_drop_view".equalsIgnoreCase((String) name));
        assertFalse(viewExists, "View should not exist after dropping");
    }

    @Test
    public void testQueryView()
    {
        assertUpdate("CREATE VIEW test_query_view AS SELECT orderkey, custkey, totalprice FROM orders WHERE orderkey <= 10");

        MaterializedResult result = computeActual("SELECT orderkey, custkey FROM test_query_view WHERE orderkey = 1");
        assertEquals(result.getRowCount(), 1);
        result = computeActual("SELECT COUNT(*) FROM test_query_view");
        assertEquals(result.getRowCount(), 1);
        long count = (Long) result.getOnlyValue();
        assertTrue(count > 0 && count <= 10, "View should contain between 1 and 10 rows");

        assertUpdate("DROP VIEW test_query_view");
    }
}
