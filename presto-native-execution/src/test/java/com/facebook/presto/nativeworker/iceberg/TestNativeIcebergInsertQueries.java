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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestNativeIcebergInsertQueries
        extends IcebergPartitionTestBase
{
    private final String basicTable = "iceberg_insert_test";
    private final String partitionedTable = "iceberg_partitioned_insert_test";

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = ((QueryRunner) getExpectedQueryRunner());
        createTestTables(queryRunner);
    }

    @BeforeMethod
    public void cleanUpTables()
    {
        assertQuerySucceeds(format("DELETE FROM %s", basicTable));
        assertQuerySucceeds(format("DELETE FROM %s", partitionedTable));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds(format("DROP TABLE IF EXISTS %s", basicTable));
        assertQuerySucceeds(format("DROP TABLE IF EXISTS %s", partitionedTable));
    }

    private void createTestTables(QueryRunner queryRunner)
    {
        queryRunner.execute(format("DROP TABLE IF EXISTS %s", basicTable));
        queryRunner.execute(format("CREATE TABLE %s (id INTEGER, name VARCHAR, value DECIMAL(10,2))", basicTable));

        queryRunner.execute(format("DROP TABLE IF EXISTS %s", partitionedTable));
        queryRunner.execute(format("CREATE TABLE %s (id INTEGER, name VARCHAR, region VARCHAR, value DECIMAL(10,2)) WITH (partitioning = ARRAY['region'])", partitionedTable));
    }

    @Test
    public void testBasicInsert()
    {
        assertUpdate(format("INSERT INTO %s VALUES (1, 'test1', DECIMAL '1.10'), (2, 'test2', DECIMAL '2.20')", basicTable), 2);
        assertQuery(
                format("SELECT * FROM %s ORDER BY id", basicTable),
                "VALUES (1, 'test1', DECIMAL '1.10'), (2, 'test2', DECIMAL '2.20')");
        assertUpdate(format("INSERT INTO %s (id, name, value) VALUES (3, 'test3', DECIMAL '3.30')", basicTable), 1);
        assertQuery(
                format("SELECT * FROM %s WHERE id = 3", basicTable),
                "VALUES (3, 'test3', DECIMAL '3.30')");
    }

    @Test
    public void testInsertIntoPartitionedTable()
    {
        assertUpdate(format("INSERT INTO %s VALUES (1, 'test1', 'region1', DECIMAL '1.10'), (2, 'test2', 'region1', DECIMAL '2.20')", partitionedTable), 2);
        assertQuery(
                format("SELECT * FROM %s WHERE region = 'region1' ORDER BY id", partitionedTable),
                "VALUES (1, 'test1', 'region1', DECIMAL '1.10'), (2, 'test2', 'region1', DECIMAL '2.20')");
        assertUpdate(format("INSERT INTO %s VALUES (3, 'test3', 'region2', DECIMAL '3.30')", partitionedTable), 1);
        assertQuery(
                format("SELECT * FROM %s WHERE region = 'region2'", partitionedTable),
                "VALUES (3, 'test3', 'region2', DECIMAL '3.30')");
    }

    @Test
    public void testInsertWithSelect()
    {
        assertUpdate(format("INSERT INTO %s VALUES (1, 'test1', DECIMAL '1.10'), (2, 'test2', DECIMAL '2.20')", basicTable), 2);
        assertUpdate(format("INSERT INTO %s SELECT id + 10, name || '_copy', CAST(value * 2 AS DECIMAL(10,2)) FROM %s", basicTable, basicTable), 2);
        assertQuery(
                format("SELECT * FROM %s WHERE id > 10 ORDER BY id", basicTable),
                "VALUES (11, 'test1_copy', DECIMAL '2.20'), (12, 'test2_copy', DECIMAL '4.40')");
    }

    @Test
    public void testInsertWithNulls()
    {
        assertUpdate(format("INSERT INTO %s VALUES (1, NULL, DECIMAL '1.10'), (2, 'test2', NULL)", basicTable), 2);
        assertQuery(
                format("SELECT * FROM %s WHERE name IS NULL OR value IS NULL ORDER BY id", basicTable),
                "VALUES (1, NULL, DECIMAL '1.10'), (2, 'test2', NULL)");
    }

    @Test
    public void testInsertWithSelectIntoPartitionedTable()
    {
        assertUpdate(format("INSERT INTO %s VALUES (1, 'test1', 'region1', DECIMAL '1.10'), (2, 'test2', 'region1', DECIMAL '2.20')", partitionedTable), 2);
        assertUpdate(
                format("INSERT INTO %s SELECT id + 10, name || '_copy', region, CAST(value * 2 AS DECIMAL(10,2)) FROM %s", partitionedTable, partitionedTable),
                2);

        assertQuery(
                format("SELECT * FROM %s WHERE region = 'region1' AND id > 10 ORDER BY id", partitionedTable),
                "VALUES (11, 'test1_copy', 'region1', DECIMAL '2.20'), (12, 'test2_copy', 'region1', DECIMAL '4.40')");

        assertUpdate(format("INSERT INTO %s VALUES (3, 'test3', 'region2', DECIMAL '3.30')", partitionedTable), 1);
        assertUpdate(
                format("INSERT INTO %s SELECT id + 20, name, 'region3', value FROM %s WHERE region = 'region2'", partitionedTable, partitionedTable),
                1);

        assertQuery(
                format("SELECT * FROM %s WHERE region = 'region3'", partitionedTable),
                "VALUES (23, 'test3', 'region3', DECIMAL '3.30')");
        assertEquals(computeScalar(format("SELECT count(*) FROM %s", partitionedTable)), 6L,
                "Should have 6 rows after both inserts");
    }
}
