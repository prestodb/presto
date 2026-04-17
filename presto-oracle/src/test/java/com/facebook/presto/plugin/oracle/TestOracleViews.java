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

/**
 * Integration tests for Oracle view support.
 * Tests basic view operations: CREATE, SHOW, LIST (via SHOW TABLES), and DROP.
 */
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
        // Create test views
        assertUpdate("CREATE VIEW view1 AS SELECT orderkey FROM orders");
        assertUpdate("CREATE VIEW view2 AS SELECT custkey FROM orders");

        // List views using SHOW TABLES (views should appear as tables)
        MaterializedResult result = computeActual("SHOW TABLES");
        List<Object> tableNames = new ArrayList<>(result.getOnlyColumnAsSet());

        boolean foundView1 = false;
        boolean foundView2 = false;
        for (Object tableName : tableNames) {
            String name = (String) tableName;
            if ("view1".equalsIgnoreCase(name)) {
                foundView1 = true;
            }
            if ("view2".equalsIgnoreCase(name)) {
                foundView2 = true;
            }
        }

        assertTrue(foundView1, "view1 should be listed in SHOW TABLES");
        assertTrue(foundView2, "view2 should be listed in SHOW TABLES");

        // Cleanup
        assertUpdate("DROP VIEW view1");
        assertUpdate("DROP VIEW view2");
    }

    @Test
    public void testDropView()
    {
        // Create view
        assertUpdate("CREATE VIEW test_drop_view AS SELECT orderkey FROM orders");

        // Verify view exists
        MaterializedResult result = computeActual("SHOW TABLES");
        List<Object> tableNames = new ArrayList<>(result.getOnlyColumnAsSet());
        boolean viewExists = tableNames.stream()
                .anyMatch(name -> "test_drop_view".equalsIgnoreCase((String) name));
        assertTrue(viewExists, "View should exist after creation");

        // Drop view
        assertUpdate("DROP VIEW test_drop_view");

        // Verify view no longer exists
        result = computeActual("SHOW TABLES");
        tableNames = new ArrayList<>(result.getOnlyColumnAsSet());
        viewExists = tableNames.stream()
                .anyMatch(name -> "test_drop_view".equalsIgnoreCase((String) name));
        assertFalse(viewExists, "View should not exist after dropping");
    }
}
