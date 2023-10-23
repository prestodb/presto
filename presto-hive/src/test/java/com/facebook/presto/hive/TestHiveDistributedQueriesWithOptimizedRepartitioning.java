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

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestHiveDistributedQueriesWithOptimizedRepartitioning
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                getTables(),
                ImmutableMap.of(
                        "experimental.optimized-repartitioning", "true",
                        // Use small SerializedPages to force flushing
                        "driver.max-page-partitioning-buffer-size", "10000B"),
                Optional.empty());
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

    @Test
    public void testQuotedIdentifiers()
    {
        // Expected to fail as Table is stored in Uppercase in H2 db and exists in tpch as lowercase
        assertQueryFails("SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"", "Table hive.tpch.ORDERS does not exist");
    }

    @Test
    public void testRenameTable()
    {
        assertUpdate("CREATE TABLE test_rename AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename RENAME TO test_rename_new");
        MaterializedResult materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE IF EXISTS test_rename_new RENAME TO test_rename");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE IF EXISTS test_rename RENAME TO test_rename_new");
        materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        // provide new table name in uppercase
        assertUpdate("ALTER TABLE test_rename_new RENAME TO TEST_RENAME");
        materializedRows = computeActual("SELECT x FROM TEST_RENAME");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("DROP TABLE TEST_RENAME");

        assertFalse(getQueryRunner().tableExists(getSession(), "TEST_RENAME"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));

        assertUpdate("ALTER TABLE IF EXISTS test_rename RENAME TO test_rename_new");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));
    }
    // Hive specific tests should normally go in TestHiveIntegrationSmokeTest
}
