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
package com.facebook.presto.tests;

import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestLocalQueries.createLocalQueryRunner;

/**
 * Tests for EXPLAIN CREATE TABLE and EXPLAIN DROP TABLE validation.
 */
public class TestExplainCreateDropTable
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createLocalQueryRunner();
    }

    @BeforeClass
    public void setUp()
    {
        // Create a test table for our tests
        assertUpdate("CREATE TABLE test_explain_table (id INTEGER, name VARCHAR)");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        // Clean up the test table
        assertUpdate("DROP TABLE IF EXISTS test_explain_table");
        assertUpdate("DROP TABLE IF EXISTS test_new_table");
    }

    @Test
    public void testExplainCreateTableAlreadyExists()
    {
        // EXPLAIN CREATE TABLE should fail when table already exists
        assertQueryFails(
                "EXPLAIN CREATE TABLE test_explain_table (id INTEGER)",
                ".*Table.*test_explain_table.*already exists.*");
    }

    @Test
    public void testExplainCreateTableIfNotExistsAlreadyExists()
    {
        // EXPLAIN CREATE TABLE IF NOT EXISTS should succeed even if table exists
        assertExplainAnalyze("EXPLAIN CREATE TABLE IF NOT EXISTS test_explain_table (id INTEGER)");
    }

    @Test
    public void testExplainCreateTableIfNotExistsNonExisting()
    {
        // EXPLAIN CREATE TABLE IF NOT EXISTS should succeed when table doesn't exist
        assertExplainAnalyze("EXPLAIN CREATE TABLE IF NOT EXISTS test_new_table (id INTEGER)");
    }

    @Test
    public void testExplainCreateTableNonExisting()
    {
        // EXPLAIN CREATE TABLE should succeed when table doesn't exist
        assertExplainAnalyze("EXPLAIN CREATE TABLE test_new_table (id INTEGER)");
    }

    @Test
    public void testExplainDropTableNonExisting()
    {
        // EXPLAIN DROP TABLE should fail when table doesn't exist
        assertQueryFails(
                "EXPLAIN DROP TABLE non_existing_table",
                ".*Table.*non_existing_table.*does not exist.*");
    }

    @Test
    public void testExplainDropTableIfExistsNonExisting()
    {
        // EXPLAIN DROP TABLE IF EXISTS should succeed even if table doesn't exist
        assertExplainAnalyze("EXPLAIN DROP TABLE IF EXISTS non_existing_table");
    }

    @Test
    public void testExplainDropTableIfExistsExisting()
    {
        // EXPLAIN DROP TABLE IF EXISTS should succeed when table exists
        assertExplainAnalyze("EXPLAIN DROP TABLE IF EXISTS test_explain_table");
    }

    @Test
    public void testExplainDropTableExists()
    {
        // EXPLAIN DROP TABLE should succeed when table exists
        assertExplainAnalyze("EXPLAIN DROP TABLE test_explain_table");
    }
}
