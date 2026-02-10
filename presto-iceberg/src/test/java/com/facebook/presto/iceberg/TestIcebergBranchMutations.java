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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestIcebergBranchMutations
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_SCHEMA = "test_schema_branch_mutations";
    private Session session;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();

        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setSchemaName(TEST_SCHEMA)
                .setCreateTpchTables(false)
                .build().getQueryRunner();
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate(session, format("CREATE SCHEMA IF NOT EXISTS %s", TEST_SCHEMA));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate(session, format("DROP SCHEMA IF EXISTS %s", TEST_SCHEMA));
    }

    private void createTable(String tableName)
    {
        assertUpdate(session, "CREATE TABLE IF NOT EXISTS " + tableName + " (id BIGINT, name VARCHAR, value INTEGER) WITH (format = 'PARQUET')");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds(session, "DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    public void testInsertIntoBranch()
    {
        String tableName = "test_insert_branch";
        createTable(tableName);
        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'audit_branch'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" WHERE name = 'audit_branch' AND type = 'BRANCH'", "VALUES 1");
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'audit_branch'", "VALUES 2");
            // Insert into branch
            assertUpdate(session, "INSERT INTO \"" + tableName + ".branch_audit_branch\" VALUES (3, 'Charlie', 300), (4, 'David', 400)", 2);
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'audit_branch'", "VALUES 4");
            // Verify main table still has only original data
            assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 2");
            assertQuery(session, "SELECT id, name, value FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'audit_branch' WHERE id = 3", "VALUES (3, 'Charlie', 300)");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'audit_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateBranch()
    {
        String tableName = "test_update_branch";
        createTable(tableName);
        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'update_branch'");
            // Update data in branch
            assertUpdate(session, "UPDATE \"" + tableName + ".branch_update_branch\" SET value = 999 WHERE id = 1", 1);
            // Verify update in branch
            assertQuery(session, "SELECT value FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'update_branch' WHERE id = 1", "VALUES 999");
            assertQuery(session, "SELECT value FROM " + tableName + " WHERE id = 1", "VALUES 100");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'update_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testDeleteFromBranch()
    {
        String tableName = "test_delete_branch";
        createTable(tableName);
        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'delete_branch'");
            // Delete from branch
            assertUpdate(session, "DELETE FROM \"" + tableName + ".branch_delete_branch\" WHERE id = 2", 1);
            // Verify deletion in branch
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'delete_branch'", "VALUES 1");
            assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'delete_branch' WHERE id = 2", "VALUES 0");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'delete_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMultipleMutationsOnBranch()
    {
        String tableName = "test_multiple_mutations_branch";
        createTable(tableName);
        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'multi_branch'");
            // Perform multiple operations on branch
            assertUpdate(session, "INSERT INTO \"" + tableName + ".branch_multi_branch\" VALUES (3, 'Charlie', 300)", 1);
            assertUpdate(session, "UPDATE \"" + tableName + ".branch_multi_branch\" SET value = 150 WHERE id = 1", 1);
            assertUpdate(session, "DELETE FROM \"" + tableName + ".branch_multi_branch\" WHERE id = 2", 1);
            // Verify final state in branch
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'multi_branch'", "VALUES 2");
            assertQuery(session, "SELECT id, name, value FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'multi_branch' ORDER BY id", "VALUES (1, 'Alice', 150), (3, 'Charlie', 300)");
            assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'multi_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertIntoBranchWithColumnList()
    {
        String tableName = "test_insert_branch_columns";
        createTable(tableName);
        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'column_branch'");
            // Insert with column list
            assertUpdate(session, "INSERT INTO \"" + tableName + ".branch_column_branch\" (id, name, value) VALUES (5, 'Eve', 500)", 1);
            assertQuery(session, "SELECT id, name, value FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'column_branch' WHERE id = 5", "VALUES (5, 'Eve', 500)");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'column_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateBranchWithComplexWhere()
    {
        String tableName = "test_update_branch_complex";
        createTable(tableName);
        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'complex_branch'");
            // Update with complex WHERE clause
            assertUpdate(session, "UPDATE \"" + tableName + ".branch_complex_branch\" SET value = value * 2 WHERE value > 100", 1);
            assertQuery(session, "SELECT value FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'complex_branch' WHERE id = 2", "VALUES 400");
            assertQuery(session, "SELECT value FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'complex_branch' WHERE id = 1", "VALUES 100");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'complex_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testDeleteFromBranchWithComplexWhere()
    {
        String tableName = "test_delete_branch_complex";
        createTable(tableName);
        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'delete_complex_branch'");
            // Delete with complex WHERE clause
            assertUpdate(session, "DELETE FROM \"" + tableName + ".branch_delete_complex_branch\" WHERE value >= 200", 1);
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'delete_complex_branch'", "VALUES 1");
            assertQuery(session, "SELECT id FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'delete_complex_branch'", "VALUES 1");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'delete_complex_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testBranchIsolation()
    {
        String tableName = "test_branch_isolation";
        createTable(tableName);
        try {
            // Create two branches
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'branch_a'");
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'branch_b'");
            // Insert different data into each branch
            assertUpdate(session, "INSERT INTO \"" + tableName + ".branch_branch_a\" VALUES (10, 'Branch A', 1000)", 1);
            assertUpdate(session, "INSERT INTO \"" + tableName + ".branch_branch_b\" VALUES (20, 'Branch B', 2000)", 1);
            // Verify isolation - branch_a should not see branch_b's data
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'branch_a' WHERE id = 20", "VALUES 0");
            // Verify isolation - branch_b should not see branch_a's data
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'branch_b' WHERE id = 10", "VALUES 0");
            // Verify each branch has its own data
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'branch_a' WHERE id = 10", "VALUES 1");
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'branch_b' WHERE id = 20", "VALUES 1");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'branch_a'");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'branch_b'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertIntoBranchFromSelect()
    {
        String tableName = "test_insert_branch_select";
        String sourceTable = "test_insert_branch_source";
        createTable(tableName);
        createTable(sourceTable);
        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'select_branch'");
            assertUpdate(session, "INSERT INTO \"" + tableName + ".branch_select_branch\" SELECT * FROM " + sourceTable, 2);
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'select_branch'", "VALUES 4");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'select_branch'");
        }
        finally {
            dropTable(tableName);
            dropTable(sourceTable);
        }
    }
}
