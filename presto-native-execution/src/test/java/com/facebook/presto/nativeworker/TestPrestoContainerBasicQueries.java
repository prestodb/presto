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
package com.facebook.presto.nativeworker;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPrestoContainerBasicQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerQueryRunner createQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunner();
    }

    @Test
    public void testPresenceAndBasicFunctionality()
    {
        // Test for native worker presence
        assertTrue(
                computeActual("SELECT * FROM system.runtime.nodes").toString().contains("native-worker-1"),
                "Native worker is not present.");

        // Test for native runtime tasks presence
        assertTrue(
                computeActual("SELECT * FROM system.runtime.tasks").toString().contains("native-worker-1"),
                "Native worker is not present.");

        // Test for tpch catalog presence
        assertTrue(
                computeActual("SHOW catalogs").toString().contains("tpch"),
                "tpch catalog is not present.");

        // Test for specific session presence
        assertTrue(
                computeActual("SHOW session").toString().contains("native_aggregation_spill_all"),
                "native_aggregation_spill_all is not present.");
    }

    @Test
    public void testBasicSQLOperations()
    {
        // Test for join operation
        assertEquals(
                computeActual(
                        "SELECT c.c_name, c.c_address, o.o_orderdate FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey ORDER BY o.o_orderdate DESC")
                        .getMaterializedRows()
                        .size(),
                172);

        // Test for count function
        assertQuery("SELECT COUNT(*) FROM (VALUES 1, 0, 0, 2, 3, 3) as t(x)", "SELECT 6");
    }

    @Test
    public void testArraySortFunction()
    {
        assertQuery("SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "SELECT ARRAY[3, 5, 5, 20, 50, null]");
    }

    @Test
    public void testUnnestOperations()
    {
        // Simple array unnesting tests
        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a)", "SELECT 1");
        assertQuery("SELECT x[1] FROM UNNEST(ARRAY[ARRAY[1, 2, 3]]) t(x)", "SELECT 1");
        assertQuery("SELECT x[1][2] FROM UNNEST(ARRAY[ARRAY[ARRAY[1, 2, 3]]]) t(x)", "SELECT 2");
        assertQuery("SELECT x[2] FROM UNNEST(ARRAY[MAP(ARRAY[1,2], ARRAY['hello', 'hi'])]) t(x)", "SELECT 'hi'");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3])", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3]) t(a)", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2], ARRAY[3, 4]) t(a, b)", "SELECT * FROM VALUES (1, 3), (2, 4)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 1, 2, 3");
        assertQuery("SELECT count(*) FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5])", "SELECT 3");
        assertQuery("SELECT a FROM UNNEST(ARRAY['kittens', 'puppies']) t(a)", "SELECT * FROM VALUES ('kittens'), ('puppies')");

        // UNNEST with UNION and CROSS JOIN
        assertQuery(
                "WITH unioned AS ( SELECT 1 UNION ALL SELECT 2 ) SELECT * FROM unioned CROSS JOIN UNNEST(ARRAY[3]) steps (step)",
                "SELECT * FROM (VALUES (1, 3), (2, 3))");
        assertQuery(
                "SELECT c FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b) CROSS JOIN (values (8), (9)) t2(c)",
                "SELECT * FROM VALUES 8, 8, 8, 9, 9, 9");

        // Multiple UNNEST functions
        assertQuery(
                "SELECT * FROM UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1)");
        assertQuery(
                "SELECT * FROM UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1)");

        // UNNEST with map
        assertQuery(
                "SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', 'dog'])) t(a, b)",
                "SELECT * FROM VALUES (1, 'cat'), (2, 'dog')");

        // UNNEST with WITH ORDINALITY
        assertQuery(
                "SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a) WITH ORDINALITY",
                "SELECT 1");
        assertQuery(
                "SELECT * FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY",
                "SELECT * FROM VALUES (1, 1), (2, 2), (3, 3)");
        assertQuery(
                "SELECT b FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY t(a, b)",
                "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery(
                "SELECT a, b FROM UNNEST(ARRAY['kittens', 'puppies']) WITH ORDINALITY t(a, b)",
                "SELECT * FROM VALUES ('kittens', 1), ('puppies', 2)");
        assertQuery(
                "SELECT c FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c) CROSS JOIN (values (8), (9)) t2(d)",
                "SELECT * FROM VALUES 1, 1, 2, 2, 3, 3");
    }
}
