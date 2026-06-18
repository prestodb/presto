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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNationWithFormat;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestNativeIcebergPositionDeleteQueries
        extends AbstractTestQueryFramework
{
    private final String storageFormat = "PARQUET";

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();

        createNationWithFormat(queryRunner, storageFormat);
    }

    @Test
    public void testIcebergReadPositionDeletesOnNonPartitionedTable()
    {
        QueryRunner javaIcebergQueryRunner = (QueryRunner) getExpectedQueryRunner();

        try {
            javaIcebergQueryRunner.execute("CREATE TABLE iceberg_native_position_delete_test AS SELECT * FROM nation");

            // ASSERT number of rows in the table before delete is executed
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test", 25);

            // Verify that a row with nationkey = 10 exists in the table
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test where nationkey = 10", 1);

            // DELETE a ROW and create a position delete file
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_native_position_delete_test where nationkey = 10");

            // ASSERT number of rows in the table after delete is executed
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test", 24);

            // Verify that the row with nationkey = 10 does not exist after the delete operation
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test where nationkey = 10", 0);

            // Verify that a row with nationkey = 20 exists in the table
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test where nationkey = 20", 1);

            // DELETE another ROW to create a second delete file
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_native_position_delete_test where nationkey = 20");

            // ASSERT number of rows in the table after second delete operation
            // This also tests iceberg read with Multiple Delete Files
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test", 23);

            // Verify that a row with nationkey = 20 does not exist after the delete operation
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test where nationkey = 20", 0);

            // Verify that a row with nationkey = 100 does not exist in the table
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test where nationkey = 100", 0);

            // DELETE a row which does not exist
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_native_position_delete_test where nationkey = 100");

            // ASSERT number of rows in the table after third delete operation
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test", 23);

            // DELETE all rows in the table
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_native_position_delete_test where nationkey >= 0");

            // ASSERT number of rows in the table after fourth delete operation
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test", 0);

            // INSERT few NULL column values
            javaIcebergQueryRunner.execute("INSERT INTO iceberg_native_position_delete_test" +
                    " VALUES" +
                    "(NULL, 'INDIA', 1, 'NULL Value Row')," +
                    "(NULL, 'USA', 2, 'NULL Value Row')," +
                    "(NULL, 'UK', 3, 'NULL Value Row')," +
                    "(25, 'AUSTRALIA', 1, 'Non-NULL Value Row')");

            // ASSERT number of rows in the table
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test", 4);

            // DELETE a row with non-null value in a table with NULL values
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_native_position_delete_test where nationkey = 25");

            // ASSERT number of rows in the table after delete operation
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test", 3);

            // DELETE all rows with nationkey = NULL in the table
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_native_position_delete_test where nationkey is NULL");

            // ASSERT number of rows in the table after delete operation
            assertQueryResultCount("SELECT * FROM iceberg_native_position_delete_test", 0);
        }
        finally {
            javaIcebergQueryRunner.execute("DROP TABLE IF EXISTS iceberg_native_position_delete_test");
        }
    }

    @Test
    public void testIcebergReadPositionDeletesOnPartitionedTable()
    {
        QueryRunner javaIcebergQueryRunner = (QueryRunner) getExpectedQueryRunner();

        try {
            // CREATE A PARTITIONED ICEBERG v2 TABLE
            javaIcebergQueryRunner.execute(
                    "CREATE TABLE iceberg_partitioned_native_position_delete_test(" +
                            "nationkey BIGINT, " +
                            "name VARCHAR, " +
                            "comment VARCHAR, " +
                            "regionkey VARCHAR)" +
                            " WITH (partitioning = ARRAY['regionkey'])");
            javaIcebergQueryRunner.execute(
                    "INSERT INTO iceberg_partitioned_native_position_delete_test " +
                            "SELECT " +
                            "nationkey, " +
                            "name, " +
                            "comment, " +
                            "cast(regionkey as VARCHAR) " +
                            "FROM nation");

            // ASSERT number of rows in the table before delete is executed
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test", 25);

            // Verify that a row with nationkey = 10 exists in the table
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test where nationkey = 10", 1);

            // DELETE on a non-partition column
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_partitioned_native_position_delete_test where nationkey = 10");

            // ASSERT number of rows in the table after delete is executed
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test", 24);

            // Verify that the row with nationkey = 10 does not exist after the delete operation
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test where nationkey = 10", 0);

            // Verify the count of rows with regionkey = 0 before a delete operation
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test where regionkey = '0'", 5);

            // DELETE on a partition column
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_partitioned_native_position_delete_test where regionkey = '0'");

            // ASSERT number of rows in the table after delete is executed
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test", 19);

            // Verify the count of rows with regionkey = 0 after the delete operation
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test where regionkey = '0'", 0);

            // INSERT a new row with partition column value as NULL
            javaIcebergQueryRunner.execute("INSERT INTO iceberg_partitioned_native_position_delete_test" +
                    " VALUES" +
                    "(25, 'INDIA', 'NULL Value Row', NULL)," +
                    "(26, 'USA', 'NULL Value Row', NULL)," +
                    "(27, 'UK', 'NULL Value Row', NULL)," +
                    "(28, 'AUSTRALIA', 'Non-NULL Value Row', '5')");

            // ASSERT number of rows in the table
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test", 23);

            // DELETE on a partition column with NULL value
            javaIcebergQueryRunner.execute("DELETE FROM iceberg_partitioned_native_position_delete_test where regionkey is NULL");

            // ASSERT number of rows in the table after the delete operation
            assertQueryResultCount("SELECT * FROM iceberg_partitioned_native_position_delete_test", 20);
        }
        finally {
            javaIcebergQueryRunner.execute("DROP TABLE IF EXISTS iceberg_partitioned_native_position_delete_test");
        }
    }

    private void assertQueryResultCount(String sql, int expectedResultCount)
    {
        assertEquals(getQueryRunner().execute(sql).getRowCount(), expectedResultCount);
    }
}
