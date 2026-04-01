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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestIcebergSmallintTinyintTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build().getQueryRunner();
    }

    @Test
    public void testCreateTableWithSmallint()
    {
        String tableName = "test_smallint_table";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        // Create table with SMALLINT column - should succeed with conversion to INTEGER
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, small_col SMALLINT)");
        // Insert data
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, SMALLINT '100')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, SMALLINT '32767')", 1);  // Max SMALLINT value
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, SMALLINT '-32768')", 1); // Min SMALLINT value
        // Verify data
        MaterializedResult result = computeActual("SELECT * FROM " + tableName + " ORDER BY id");
        assertEquals(result.getRowCount(), 3);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 100);
        assertEquals(result.getMaterializedRows().get(1).getField(1), 32767);
        assertEquals(result.getMaterializedRows().get(2).getField(1), -32768);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithTinyint()
    {
        String tableName = "test_tinyint_table";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        // Create table with TINYINT column - should succeed with conversion to INTEGER
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, tiny_col TINYINT)");
        // Insert data
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, TINYINT '50')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, TINYINT '127')", 1);  // Max TINYINT value
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, TINYINT '-128')", 1); // Min TINYINT value
        // Verify data
        MaterializedResult result = computeActual("SELECT * FROM " + tableName + " ORDER BY id");
        assertEquals(result.getRowCount(), 3);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 50);
        assertEquals(result.getMaterializedRows().get(1).getField(1), 127);
        assertEquals(result.getMaterializedRows().get(2).getField(1), -128);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithMixedIntegerTypes()
    {
        String tableName = "test_mixed_integer_types";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        // Create table with all integer types
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INTEGER, " +
                "tiny_col TINYINT, " +
                "small_col SMALLINT, " +
                "int_col INTEGER, " +
                "big_col BIGINT)");
        // Insert data
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, TINYINT '10', SMALLINT '1000', 100000, BIGINT '10000000000')", 1);
        // Verify data
        MaterializedResult result = computeActual("SELECT * FROM " + tableName);
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 10);
        assertEquals(result.getMaterializedRows().get(0).getField(2), 1000);
        assertEquals(result.getMaterializedRows().get(0).getField(3), 100000);
        assertEquals(result.getMaterializedRows().get(0).getField(4), 10000000000L);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertFromSelectWithSmallintTinyint()
    {
        String sourceTable = "test_source_table";
        String targetTable = "test_target_table";
        assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        assertUpdate("DROP TABLE IF EXISTS " + targetTable);
        // Create source table with SMALLINT and TINYINT
        assertUpdate("CREATE TABLE " + sourceTable + " (id INTEGER, small_col SMALLINT, tiny_col TINYINT)");
        assertUpdate("INSERT INTO " + sourceTable + " VALUES (1, SMALLINT '100', TINYINT '10')", 1);
        assertUpdate("INSERT INTO " + sourceTable + " VALUES (2, SMALLINT '200', TINYINT '20')", 1);
        // Create target table and insert from source
        assertUpdate("CREATE TABLE " + targetTable + " (id INTEGER, small_col SMALLINT, tiny_col TINYINT)");
        assertUpdate("INSERT INTO " + targetTable + " SELECT * FROM " + sourceTable, 2);
        // Verify data
        MaterializedResult result = computeActual("SELECT * FROM " + targetTable + " ORDER BY id");
        assertEquals(result.getRowCount(), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 100);
        assertEquals(result.getMaterializedRows().get(0).getField(2), 10);
        assertEquals(result.getMaterializedRows().get(1).getField(1), 200);
        assertEquals(result.getMaterializedRows().get(1).getField(2), 20);
        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testPartitionedTableWithSmallintTinyint()
    {
        String tableName = "test_partitioned_smallint_tinyint";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        // Create partitioned table with SMALLINT partition column
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INTEGER, " +
                "small_col SMALLINT, " +
                "tiny_col TINYINT, " +
                "data VARCHAR) " +
                "WITH (PARTITIONING = ARRAY['small_col'])");
        // Insert data
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, SMALLINT '10', TINYINT '1', 'data1')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, SMALLINT '20', TINYINT '2', 'data2')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, SMALLINT '10', TINYINT '3', 'data3')", 1);
        // Verify data with partition filter
        MaterializedResult result = computeActual("SELECT * FROM " + tableName + " WHERE small_col = SMALLINT '10' ORDER BY id");
        assertEquals(result.getRowCount(), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 3);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCastFromSmallintTinyintToInteger()
    {
        String tableName = "test_cast_smallint_tinyint";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        // Create table with SMALLINT and TINYINT
        assertUpdate("CREATE TABLE " + tableName + " (small_col SMALLINT, tiny_col TINYINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (SMALLINT '100', TINYINT '10')", 1);
        // Query with explicit cast to INTEGER
        MaterializedResult result = computeActual("SELECT CAST(small_col AS INTEGER), CAST(tiny_col AS INTEGER) FROM " + tableName);
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 100);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 10);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testArithmeticOperationsWithSmallintTinyint()
    {
        String tableName = "test_arithmetic_smallint_tinyint";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        // Create table
        assertUpdate("CREATE TABLE " + tableName + " (small_col SMALLINT, tiny_col TINYINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (SMALLINT '100', TINYINT '10')", 1);
        // Test arithmetic operations
        MaterializedResult result = computeActual("SELECT small_col + tiny_col, small_col - tiny_col, small_col * tiny_col FROM " + tableName);
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 110);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 90);
        assertEquals(result.getMaterializedRows().get(0).getField(2), 1000);
        assertUpdate("DROP TABLE " + tableName);
    }
}
