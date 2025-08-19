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

import com.facebook.presto.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestPartitionedWritesAsSelect
        extends IcebergPartitionTestBase
{
    private final String sourceTable = "iceberg.tpch.source_table";
    private final String targetTable = "iceberg.tpch.target_table";
    private final int kRows = 5;

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + targetTable);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + sourceTable);
        assertUpdate("CREATE TABLE " + sourceTable + " (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) WITH (format = 'PARQUET')");
    }

    @BeforeMethod
    public void cleanUpTables()
    {
        assertQuerySucceeds("DELETE FROM " + sourceTable);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + targetTable);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + sourceTable);
    }

    @Test
    public void testInsertAsSelectAppend()
    {
        int repeats = 3;
        long totalRows = repeats * kRows;
        insertData(3);
        MaterializedResult expected = computeActual("SELECT * FROM " + sourceTable + " ORDER BY id");
        assertUpdate("CREATE TABLE " + targetTable + " (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['event_date', 'category'])");
        assertUpdate("INSERT INTO " + targetTable + " SELECT id, data, category, event_date FROM " + sourceTable + " ORDER BY event_date, category", totalRows);
        assertEquals(computeScalar("SELECT count(*) FROM " + targetTable), totalRows, "Should have 15 rows after insert");
        MaterializedResult actual = computeActual("SELECT * FROM " + targetTable + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testInsertAsSelectWithBucket()
    {
        int repeats = 3;
        long totalRows = repeats * kRows;
        insertData(repeats);
        MaterializedResult expected = computeActual("SELECT * FROM " + sourceTable + " ORDER BY id");
        assertUpdate("CREATE TABLE " + targetTable + " (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['bucket(data, 8)'])");
        assertUpdate("INSERT INTO " + targetTable + " SELECT id, data, category, event_date FROM " + sourceTable, totalRows);
        assertEquals(computeScalar("SELECT count(*) FROM " + targetTable), totalRows, "Should have 15 rows after insert");
        MaterializedResult actual = computeActual("SELECT * FROM " + targetTable + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testInsertAsSelectWithTruncate()
    {
        int repeats = 3;
        long totalRows = repeats * kRows;
        insertData(repeats);
        MaterializedResult expected = computeActual("SELECT * FROM " + sourceTable + " ORDER BY id");
        assertUpdate("CREATE TABLE " + targetTable + " (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['truncate(data, 4)', 'truncate(id, 4)'])");

        assertUpdate("INSERT INTO " + targetTable + " SELECT id, data, category, event_date FROM " + sourceTable, totalRows);
        assertEquals(computeScalar("SELECT count(*) FROM " + targetTable), totalRows, "Should have 15 rows after insert");
        MaterializedResult actual = computeActual("SELECT * FROM " + targetTable + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    private void insertData(int repeatCounter)
    {
        for (int i = 0; i < repeatCounter; i++) {
            assertUpdate(
                    "INSERT INTO " + sourceTable + " VALUES " +
                            "(13, '1', 'bgd16', DATE '2021-11-10'), " +
                            "(21, '2', 'bgd13', DATE '2021-11-10'), " +
                            "(12, '3', 'bgd14', DATE '2021-11-10'), " +
                            "(222, '3', 'bgd15', DATE '2021-11-10'), " +
                            "(45, '4', 'bgd16', DATE '2021-11-10')",
                    5);
        }
    }

    @Test
    public void testInsertWithSelectIntoPartitionedTable()
    {
        int repeats = 2;
        long totalRows = repeats * kRows;
        insertData(repeats);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + targetTable);
        assertUpdate("CREATE TABLE " + targetTable + " (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['category'])");
        assertUpdate("INSERT INTO " + targetTable + " SELECT id, data, category, event_date FROM " + sourceTable, totalRows);
        assertUpdate(
                "INSERT INTO " + targetTable +
                        " SELECT id + 1000, data || '_copy', category, event_date FROM " + sourceTable +
                        " WHERE category LIKE 'bgd1%'",
                totalRows);
        assertEquals(computeScalar("SELECT count(*) FROM " + targetTable), totalRows * 2,
                "Should have " + (totalRows * 2) + " rows after both inserts");
        MaterializedResult transformedData = computeActual(
                "SELECT * FROM " + targetTable + " WHERE id > 1000 ORDER BY id LIMIT 5");

        assertEquals(transformedData.getMaterializedRows().get(0).getField(0), 1012L); // id + 1000
        assertEquals(transformedData.getMaterializedRows().get(0).getField(1), "3_copy"); // data || '_copy'
        assertEquals(transformedData.getMaterializedRows().get(0).getField(2), "bgd14"); // category unchanged

        long countInBgd16 = (long) computeScalar("SELECT count(*) FROM " + targetTable + " WHERE category = 'bgd16'");
        assertEquals(countInBgd16, 8, "Should have 8 rows in the 'bgd16' partition");
    }
}
