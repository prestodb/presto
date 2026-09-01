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
import static java.lang.String.format;

public class TestPartitionedWrite
        extends IcebergPartitionTestBase
{
    private static final String SOURCE_TABLE = "iceberg.tpch.source_table";
    private static final String TARGET_TABLE = "iceberg.tpch.target_table";
    private static final int ROWS_PER_INSERT = 5;

    @BeforeClass
    public void setUp()
    {
        dropTableSafely(TARGET_TABLE);
        dropTableSafely(SOURCE_TABLE);
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET')", SOURCE_TABLE));
    }

    @BeforeMethod
    public void cleanUpTables()
    {
        assertQuerySucceeds("DELETE FROM " + SOURCE_TABLE);
        dropTableSafely(TARGET_TABLE);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        dropTableSafely(SOURCE_TABLE);
        dropTableSafely(TARGET_TABLE);
    }

    @Test
    public void testIdentityTransform()
    {
        int repeats = 3;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestData(repeats);

        MaterializedResult expected = computeActual("SELECT * FROM " + SOURCE_TABLE + " ORDER BY id");
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['event_date', 'category'])", TARGET_TABLE));
        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s ORDER BY event_date, category",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows);
        MaterializedResult actual = computeActual("SELECT * FROM " + TARGET_TABLE + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testIdentityMultiInsert()
    {
        int repeats = 2;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestData(repeats);

        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['category'])", TARGET_TABLE));

        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        assertUpdate(format("INSERT INTO %s SELECT id + 1000, data || '_copy', category, event_date FROM %s WHERE category LIKE 'bgd1%%'",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows * 2);

        MaterializedResult transformedData = computeActual(
                "SELECT * FROM " + TARGET_TABLE + " WHERE id > 1000 ORDER BY id LIMIT 5");
        assertEquals(transformedData.getMaterializedRows().get(0).getField(0), 1012L, "id should be original + 1000");
        assertEquals(transformedData.getMaterializedRows().get(0).getField(1), "3_copy", "data should have '_copy' suffix");
        assertEquals(transformedData.getMaterializedRows().get(0).getField(2), "bgd14", "category should be unchanged");

        long countInBgd16 = (long) computeScalar("SELECT count(*) FROM " + TARGET_TABLE + " WHERE category = 'bgd16'");
        assertEquals(countInBgd16, 8L, "Should have 8 rows in the 'bgd16' partition");
    }

    @Test
    public void testBucketTransform()
    {
        int repeats = 3;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestData(repeats);

        MaterializedResult expected = computeActual("SELECT * FROM " + SOURCE_TABLE + " ORDER BY id");
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['bucket(data, 8)'])", TARGET_TABLE));
        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows);
        MaterializedResult actual = computeActual("SELECT * FROM " + TARGET_TABLE + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testBucketTransformOnDate()
    {
        int repeats = 3;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestDataWithMultipleDates(repeats);

        MaterializedResult expected = computeActual("SELECT * FROM " + SOURCE_TABLE + " ORDER BY id");
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['bucket(event_date, 4)'])", TARGET_TABLE));
        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows);
        MaterializedResult actual = computeActual("SELECT * FROM " + TARGET_TABLE + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testTruncateTransform()
    {
        int repeats = 3;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestData(repeats);

        MaterializedResult expected = computeActual("SELECT * FROM " + SOURCE_TABLE + " ORDER BY id");
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['truncate(data, 4)', 'truncate(id, 4)'])", TARGET_TABLE));
        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows);
        MaterializedResult actual = computeActual("SELECT * FROM " + TARGET_TABLE + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testYearTransform()
    {
        int repeats = 3;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestDataWithMultipleYears(repeats);

        MaterializedResult expected = computeActual("SELECT * FROM " + SOURCE_TABLE + " ORDER BY id");
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['year(event_date)'])", TARGET_TABLE));
        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows);
        MaterializedResult actual = computeActual("SELECT * FROM " + TARGET_TABLE + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testMonthTransform()
    {
        int repeats = 3;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestDataWithMultipleDates(repeats);

        MaterializedResult expected = computeActual("SELECT * FROM " + SOURCE_TABLE + " ORDER BY id");
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['month(event_date)'])", TARGET_TABLE));
        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows);
        MaterializedResult actual = computeActual("SELECT * FROM " + TARGET_TABLE + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testDayTransform()
    {
        int repeats = 3;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestDataWithMultipleDates(repeats);

        MaterializedResult expected = computeActual("SELECT * FROM " + SOURCE_TABLE + " ORDER BY id");
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['day(event_date)'])", TARGET_TABLE));
        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows);
        MaterializedResult actual = computeActual("SELECT * FROM " + TARGET_TABLE + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    @Test
    public void testCombinedTransforms()
    {
        int repeats = 3;
        long totalRows = repeats * ROWS_PER_INSERT;
        insertTestDataWithMultipleYears(repeats);

        MaterializedResult expected = computeActual("SELECT * FROM " + SOURCE_TABLE + " ORDER BY id");
        assertUpdate(format("CREATE TABLE %s (id BIGINT, data VARCHAR, category VARCHAR, event_date DATE) " +
                "WITH (format = 'PARQUET', partitioning = ARRAY['year(event_date)', 'bucket(category, 4)', 'data'])", TARGET_TABLE));
        assertUpdate(format("INSERT INTO %s SELECT id, data, category, event_date FROM %s",
                TARGET_TABLE, SOURCE_TABLE), totalRows);

        verifyRowCount(TARGET_TABLE, totalRows);
        MaterializedResult actual = computeActual("SELECT * FROM " + TARGET_TABLE + " ORDER BY id");
        assertEquals(actual, expected, "Row data should match expected");
    }

    private void insertTestData(int repeat)
    {
        for (int i = 0; i < repeat; i++) {
            assertUpdate(format("INSERT INTO %s VALUES " +
                            "(13, '1', 'bgd16', DATE '2021-11-10'), " +
                            "(21, '2', 'bgd13', DATE '2021-11-10'), " +
                            "(12, '3', 'bgd14', DATE '2021-11-10'), " +
                            "(222, '3', 'bgd15', DATE '2021-11-10'), " +
                            "(45, '4', 'bgd16', DATE '2021-11-10')",
                    SOURCE_TABLE), ROWS_PER_INSERT);
        }
    }

    private void insertTestDataWithMultipleDates(int repeat)
    {
        for (int i = 0; i < repeat; i++) {
            assertUpdate(format("INSERT INTO %s VALUES " +
                            "(13, '1', 'bgd16', DATE '2021-01-10'), " +
                            "(21, '2', 'bgd13', DATE '2021-02-15'), " +
                            "(12, '3', 'bgd14', DATE '2021-03-20'), " +
                            "(222, '3', 'bgd15', DATE '2021-04-25'), " +
                            "(45, '4', 'bgd16', DATE '2021-05-30')",
                    SOURCE_TABLE), ROWS_PER_INSERT);
        }
    }

    private void insertTestDataWithMultipleYears(int repeat)
    {
        for (int i = 0; i < repeat; i++) {
            assertUpdate(format("INSERT INTO %s VALUES " +
                            "(13, '1', 'bgd16', DATE '2020-11-10'), " +
                            "(21, '2', 'bgd13', DATE '2021-11-10'), " +
                            "(12, '3', 'bgd14', DATE '2022-11-10'), " +
                            "(222, '3', 'bgd15', DATE '2023-11-10'), " +
                            "(45, '4', 'bgd16', DATE '2024-11-10')",
                    SOURCE_TABLE), ROWS_PER_INSERT);
        }
    }

    private void verifyRowCount(String tableName, long expectedCount)
    {
        long actualCount = (long) computeScalar("SELECT count(*) FROM " + tableName);
        assertEquals(actualCount, expectedCount, format("Table %s should have %d rows", tableName, expectedCount));
    }
}
