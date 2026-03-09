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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static org.testng.Assert.assertEquals;

public class TestPrestoNativeIcebergTagsAndBranches
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testQueryBranch()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_branch_dot_notation");
        assertUpdate("CREATE TABLE test_branch_dot_notation (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO test_branch_dot_notation VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);
        assertQuerySucceeds("ALTER TABLE test_branch_dot_notation CREATE BRANCH 'audit_branch'");
        assertUpdate("INSERT INTO test_branch_dot_notation VALUES (3, 'Charlie', 300), (4, 'David', 400)", 2);
        // Test querying branch using FOR SYSTEM_VERSION AS OF syntax
        assertQuery("SELECT count(*) FROM test_branch_dot_notation FOR SYSTEM_VERSION AS OF 'audit_branch'", "VALUES 2");
        assertQuery("SELECT count(*) FROM test_branch_dot_notation FOR SYSTEM_VERSION AS OF 'main'", "VALUES 4");
        // Test querying branch using dot notation syntax
        assertQuery("SELECT count(*) FROM \"test_branch_dot_notation.branch_audit_branch\"", "VALUES 2");
        assertQuery("SELECT id, name, value FROM \"test_branch_dot_notation.branch_audit_branch\" ORDER BY id", "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        // Verify both syntaxes return the same results by comparing actual results
        MaterializedResult resultWithForSyntax = computeActual("SELECT id FROM test_branch_dot_notation FOR SYSTEM_VERSION AS OF 'audit_branch' ORDER BY id");
        MaterializedResult resultWithDotNotation = computeActual("SELECT id FROM \"test_branch_dot_notation.branch_audit_branch\" ORDER BY id");
        assertEquals(resultWithForSyntax, resultWithDotNotation);
        // Test that main table has all records
        assertQuery("SELECT count(*) FROM test_branch_dot_notation", "VALUES 4");
    }

    @Test
    public void testQueryTag()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_query_tag");
        assertUpdate("CREATE TABLE test_query_tag (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO test_query_tag VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);
        assertQuerySucceeds("ALTER TABLE test_query_tag CREATE TAG 'audit_tag'");
        assertUpdate("INSERT INTO test_query_tag VALUES (3, 'Charlie', 300), (4, 'David', 400)", 2);
        // Test querying tag using FOR SYSTEM_VERSION AS OF syntax
        assertQuery("SELECT count(*) FROM test_query_tag FOR SYSTEM_VERSION AS OF 'audit_tag'", "VALUES 2");
        assertQuery("SELECT count(*) FROM test_query_tag FOR SYSTEM_VERSION AS OF 'main'", "VALUES 4");
        // Verify tag returns correct data
        assertQuery("SELECT id, name, value FROM test_query_tag FOR SYSTEM_VERSION AS OF 'audit_tag' ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");
        // Test that main table has all records
        assertQuery("SELECT count(*) FROM test_query_tag", "VALUES 4");
    }

    @Test
    public void testCreateTag()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_create_tag");
        assertUpdate("CREATE TABLE test_create_tag (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_create_tag VALUES (1, 'Alice'), (2, 'Bob')", 2);
        // Create tag on current snapshot
        assertQuerySucceeds("ALTER TABLE test_create_tag CREATE TAG 'audit_tag'");
        assertUpdate("INSERT INTO test_create_tag VALUES (3, 'Charlie')", 1);
        // Verify tag points to the snapshot before the last insert
        assertQuery("SELECT count(*) FROM test_create_tag FOR SYSTEM_VERSION AS OF 'audit_tag'", "VALUES 2");
        assertQuery("SELECT count(*) FROM test_create_tag", "VALUES 3");
        // Verify we can query the refs table to see created tags
        assertQuery("SELECT count(*) FROM \"test_create_tag$refs\" WHERE type = 'TAG'", "VALUES 1");
    }

    @Test
    public void testCreateBranch()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_create_branch");
        assertUpdate("CREATE TABLE test_create_branch (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_create_branch VALUES (1, 'Alice'), (2, 'Bob')", 2);
        // Create branch on current snapshot
        assertQuerySucceeds("ALTER TABLE test_create_branch CREATE BRANCH 'dev_branch'");
        assertUpdate("INSERT INTO test_create_branch VALUES (3, 'Charlie')", 1);
        // Verify branch points to the snapshot before the last insert
        assertQuery("SELECT count(*) FROM test_create_branch FOR SYSTEM_VERSION AS OF 'dev_branch'", "VALUES 2");
        assertQuery("SELECT count(*) FROM test_create_branch", "VALUES 3");
        // Verify we can query the refs table to see created branches
        assertQuery("SELECT count(*) FROM \"test_create_branch$refs\" WHERE type = 'BRANCH'", "VALUES 2"); // main + dev_branch
    }

    @Test
    public void testDropTag()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_drop_tag");
        assertUpdate("CREATE TABLE test_drop_tag (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_drop_tag VALUES (1, 'Alice'), (2, 'Bob')", 2);
        assertQuerySucceeds("ALTER TABLE test_drop_tag CREATE TAG 'tag1'");
        assertUpdate("INSERT INTO test_drop_tag VALUES (3, 'Charlie')", 1);
        assertQuerySucceeds("ALTER TABLE test_drop_tag CREATE TAG 'tag2'");
        // Verify both tags exist
        assertQuery("SELECT count(*) FROM \"test_drop_tag$refs\" WHERE type = 'TAG'", "VALUES 2");
        // Drop tag1
        assertQuerySucceeds("ALTER TABLE test_drop_tag DROP TAG 'tag1'");
        assertQuery("SELECT count(*) FROM \"test_drop_tag$refs\" WHERE type = 'TAG'", "VALUES 1");
        // Verify tag1 is dropped and tag2 still exists
        assertQueryFails("SELECT count(*) FROM test_drop_tag FOR SYSTEM_VERSION AS OF 'tag1'", ".*tag1.*");
        assertQuery("SELECT count(*) FROM test_drop_tag FOR SYSTEM_VERSION AS OF 'tag2'", "VALUES 3");
        // Drop non-existent tag should fail
        assertQueryFails("ALTER TABLE test_drop_tag DROP TAG 'non_existent_tag'", ".*non_existent_tag.*");
        // Drop with IF EXISTS should succeed
        assertQuerySucceeds("ALTER TABLE test_drop_tag DROP TAG IF EXISTS 'tag2'");
        assertQuerySucceeds("ALTER TABLE test_drop_tag DROP TAG IF EXISTS 'non_existent_tag'");
    }

    @Test
    public void testDropBranch()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_drop_branch");
        assertUpdate("CREATE TABLE test_drop_branch (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_drop_branch VALUES (1, 'Alice'), (2, 'Bob')", 2);
        assertQuerySucceeds("ALTER TABLE test_drop_branch CREATE BRANCH 'branch1'");
        assertUpdate("INSERT INTO test_drop_branch VALUES (3, 'Charlie')", 1);
        assertQuerySucceeds("ALTER TABLE test_drop_branch CREATE BRANCH 'branch2'");
        // Verify both branches exist (main + branch1 + branch2)
        assertQuery("SELECT count(*) FROM \"test_drop_branch$refs\" WHERE type = 'BRANCH'", "VALUES 3");
        // Drop branch1
        assertQuerySucceeds("ALTER TABLE test_drop_branch DROP BRANCH 'branch1'");
        assertQuery("SELECT count(*) FROM \"test_drop_branch$refs\" WHERE type = 'BRANCH'", "VALUES 2");
        // Verify branch1 is dropped and branch2 still exists
        assertQueryFails("SELECT count(*) FROM test_drop_branch FOR SYSTEM_VERSION AS OF 'branch1'", ".*branch1.*");
        assertQuery("SELECT count(*) FROM test_drop_branch FOR SYSTEM_VERSION AS OF 'branch2'", "VALUES 3");
        // Drop non-existent branch should fail
        assertQueryFails("ALTER TABLE test_drop_branch DROP BRANCH 'non_existent_branch'", ".*non_existent_branch.*");
        // Drop with IF EXISTS should succeed
        assertQuerySucceeds("ALTER TABLE test_drop_branch DROP BRANCH IF EXISTS 'branch2'");
        assertQuerySucceeds("ALTER TABLE test_drop_branch DROP BRANCH IF EXISTS 'non_existent_branch'");
    }

    // Note: INSERT and TRUNCATE operations on branches are currently supported in Prestissimo.
    // UPDATE, DELETE, and MERGE operations on branches using dot notation are not yet implemented
    @Test
    public void testBranchMutation()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_branch_mutation");
        assertUpdate("CREATE TABLE test_branch_mutation (id BIGINT, product VARCHAR, price DOUBLE)");
        assertUpdate("INSERT INTO test_branch_mutation VALUES (1, 'Product A', 100.00), (2, 'Product B', 200.00)", 2);
        // Create a branch
        assertQuerySucceeds("ALTER TABLE test_branch_mutation CREATE BRANCH 'audit_branch'");
        // Insert into branch using dot notation
        assertUpdate("INSERT INTO \"test_branch_mutation.branch_audit_branch\" VALUES (3, 'Product C', 300.00)", 1);
        assertQuery("SELECT count(*) FROM \"test_branch_mutation.branch_audit_branch\"", "VALUES 3");
        assertQuery("SELECT count(*) FROM test_branch_mutation", "VALUES 2"); // Main branch unchanged
        // Verify data in branch
        assertQuery("SELECT id, product, price FROM \"test_branch_mutation.branch_audit_branch\" ORDER BY id",
                "VALUES (1, 'Product A', 100.00), (2, 'Product B', 200.00), (3, 'Product C', 300.00)");
        // TRUNCATE branch - this is supported
        assertQuerySucceeds("TRUNCATE TABLE \"test_branch_mutation.branch_audit_branch\"");
        assertQuery("SELECT count(*) FROM \"test_branch_mutation.branch_audit_branch\"", "VALUES 0");
        // Verify main branch is still unchanged after TRUNCATE
        assertQuery("SELECT id, product, price FROM test_branch_mutation ORDER BY id",
                "VALUES (1, 'Product A', 100.00), (2, 'Product B', 200.00)");

        // Re-insert data for testing unsupported operations
        assertUpdate("INSERT INTO \"test_branch_mutation.branch_audit_branch\" VALUES (1, 'Product A', 100.00), (2, 'Product B', 200.00)", 2);
        assertQueryFails("UPDATE \"test_branch_mutation.branch_audit_branch\" SET price = 120.00 WHERE id = 1", "(?s).*");
        assertQueryFails("DELETE FROM \"test_branch_mutation.branch_audit_branch\" WHERE id = 2", "(?s).*");
        assertUpdate("CREATE TABLE test_branch_mutation_source (id BIGINT, product VARCHAR, price DOUBLE)");
        assertUpdate("INSERT INTO test_branch_mutation_source VALUES (1, 'Product A Updated', 150.00)", 1);
        assertQueryFails("MERGE INTO \"test_branch_mutation.branch_audit_branch\" t " +
                "USING test_branch_mutation_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET price = s.price " +
                "WHEN NOT MATCHED THEN INSERT (id, product, price) VALUES (s.id, s.product, s.price)", "(?s).*");
    }
}
