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
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestNativeIcebergPartitionTransforms
        extends IcebergPartitionTestBase
{
    private static final String CREATE_MULTIPLE_PARTITION_TABLE_TEMPLATE = "CREATE TABLE %s (" +
            "  int_col INTEGER, " +
            "  bigint_col BIGINT, " +
            "  varchar_col VARCHAR, " +
            "  date_col DATE" +
            ") WITH (format = 'PARQUET', partitioning = ARRAY['bucket(int_col, 4)', 'truncate(varchar_col, 2)', 'year(date_col)'])";

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_data");
        assertQuerySucceeds(
                "CREATE TABLE test_data (" +
                        "  int_col INTEGER, " +
                        "  bigint_col BIGINT, " +
                        "  varchar_col VARCHAR, " +
                        "  varbinary_col VARBINARY, " +
                        "  date_col DATE, " +
                        "  decimal_col DECIMAL(18, 6)" +
                        ")");

        assertQuerySucceeds(
                "INSERT INTO test_data VALUES " +
                        "  (1, 1000, 'apple', X'01020304', DATE '2023-01-15', DECIMAL '123.456'), " +
                        "  (2, 2000, 'banana', X'05060708', DATE '2023-02-20', DECIMAL '234.567'), " +
                        "  (3, 3000, 'cherry', X'090A0B0C', DATE '2023-03-25', DECIMAL '345.678'), " +
                        "  (4, 4000, 'date', X'0D0E0F10', DATE '2023-04-30', DECIMAL '456.789'), " +
                        "  (5, 5000, 'elderberry', X'11121314', DATE '2023-05-05', DECIMAL '567.890'), " +
                        "  (10, 10000, 'fig', X'15161718', DATE '2023-06-10', DECIMAL '678.901'), " +
                        "  (20, 20000, 'grape', X'191A1B1C', DATE '2023-07-15', DECIMAL '789.012'), " +
                        "  (30, 30000, 'honeydew', X'1D1E1F20', DATE '2023-08-20', DECIMAL '0.000001'), " +
                        "  (40, 40000, 'imbe', X'21222324', DATE '2023-09-25', DECIMAL '1.0'), " +
                        "  (50, 50000, 'jackfruit', X'25262728', DATE '2023-10-30', DECIMAL '1.00000')");
                // After support statistics, add null rows back
                //"  (NULL, NULL, NULL, NULL, NULL, NULL), " +
                //"  (NULL, NULL, NULL, NULL, NULL, NULL)");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        // Clean up all test tables
        assertQuerySucceeds("DROP TABLE IF EXISTS test_data");
        for (String transform : new String[] {"identity", "bucket", "truncate", "year", "month", "day"}) {
            for (String dataType : new String[] {"int", "bigint", "varchar", "varbinary", "date", "decimal"}) {
                if (isValidTransformForType(transform, dataType)) {
                    String nativeTableName = getTableName(transform, dataType) + "_native";
                    String javaTableName = getTableName(transform, dataType) + "_java";
                    assertQuerySucceeds(format("DROP TABLE IF EXISTS %s", nativeTableName));
                    try {
                        ((QueryRunner) getExpectedQueryRunner()).execute(format("DROP TABLE IF EXISTS %s", javaTableName));
                    }
                    catch (Exception e) {
                        // Ignore exceptions during cleanup
                        System.err.printf("Error dropping Java table %s: %s%n", javaTableName, e.getMessage());
                    }
                }
            }
        }
    }

    // Test identity transform for all data types
    @Test
    public void testIdentityTransformInteger()
    {
        testPartitionTransform("identity", "int", null);
    }

    @Test
    public void testIdentityTransformBigint()
    {
        testPartitionTransform("identity", "bigint", null);
    }

    @Test
    public void testIdentityTransformVarchar()
    {
        testPartitionTransform("identity", "varchar", null);
    }

    @Test
    public void testIdentityTransformVarbinary()
    {
        testPartitionTransform("identity", "varbinary", null);
    }

    @Test
    public void testIdentityTransformDate()
    {
        testPartitionTransform("identity", "date", null);
    }

    @Test
    public void testIdentityTransformDecimal()
    {
        testPartitionTransform("identity", "decimal", null);
    }

    // Test bucket transform for all data types
    @Test
    public void testBucketTransformInteger()
    {
        testPartitionTransform("bucket", "int", "4");
    }

    @Test
    public void testBucketTransformBigint()
    {
        testPartitionTransform("bucket", "bigint", "4");
    }

    @Test
    public void testBucketTransformVarchar()
    {
        testPartitionTransform("bucket", "varchar", "4");
    }

    @Test
    public void testBucketTransformVarbinary()
    {
        testPartitionTransform("bucket", "varbinary", "4");
    }

    @Test
    public void testBucketTransformDecimal()
    {
        testPartitionTransform("bucket", "decimal", "4");
    }

    // Test truncate transform for all data types
    @Test
    public void testTruncateTransformInteger()
    {
        testPartitionTransform("trunc", "int", "10");
    }

    @Test
    public void testTruncateTransformBigint()
    {
        testPartitionTransform("trunc", "bigint", "1000");
    }

    @Test
    public void testTruncateTransformVarchar()
    {
        testPartitionTransform("trunc", "varchar", "3");
    }

    @Test
    public void testTruncateTransformVarbinary()
    {
        testPartitionTransform("trunc", "varbinary", "2");
    }

    @Test
    public void testTruncateTransformDecimal()
    {
        testPartitionTransform("trunc", "decimal", "100");
    }

    @Test
    public void testYearTransformDate()
    {
        testPartitionTransform("year", "date", null);
    }

    @Test
    public void testMonthTransformDate()
    {
        testPartitionTransform("month", "date", null);
    }

    @Test
    public void testDayTransformDate()
    {
        testPartitionTransform("day", "date", null);
    }

    private void testPartitionTransform(String transform, String dataType, String param)
    {
        String[] tableNames = createPartitionedTables(transform, dataType, param);
        // Collect partition information
        PartitionInfo partitionInfo = collectPartitionInfo(transform,
                tableNames[0], tableNames[1], dataType + "_col");
        assertEquals(partitionInfo.nativeUniquePartitionValues, partitionInfo.javaUniquePartitionValues,
                format("Native and Java runners should generate the same partition values for %s transform on %s",
                        transform, dataType));
        // verify data are identical
        assertQuery(format("SELECT * FROM %s", tableNames[0]), format("SELECT * FROM %s", tableNames[1]));

        // Switch query engine
        assertQuery(format("SELECT * FROM %s", tableNames[1]), format("SELECT * FROM %s", tableNames[0]));

        verifyPartitionTransform(transform, param, tableNames[0], tableNames[1], dataType + "_col", partitionInfo);
        //verifyPartitionsMetadata(tableNames[0], tableNames[1]);
    }

    // Test multiple partition transforms
    @Test
    public void testMultiplePartitionTransforms()
    {
        String tableName = TEST_TABLE_PREFIX + "multiple";
        assertQuerySucceeds(format(DROP_TABLE_TEMPLATE, tableName));
        // Create a table with multiple partition transforms
        assertQuerySucceeds(format(CREATE_MULTIPLE_PARTITION_TABLE_TEMPLATE, tableName));
        // Insert data from the test_data table
        assertQuerySucceeds(
                "INSERT INTO " + tableName + " " +
                        "SELECT int_col, bigint_col, varchar_col, date_col FROM test_data");

        // Verify the data was inserted correctly
        MaterializedResult result = computeActual(
                "SELECT * FROM " + tableName + " ORDER BY int_col");
        MaterializedResult expected = computeActual(
                "SELECT int_col, bigint_col, varchar_col, date_col FROM test_data ORDER BY int_col");
        assertEquals(result, expected, "Data should match after insertion");

        ((QueryRunner) getExpectedQueryRunner()).execute(format(DROP_TABLE_TEMPLATE, tableName));
        ((QueryRunner) getExpectedQueryRunner()).execute(format(CREATE_MULTIPLE_PARTITION_TABLE_TEMPLATE, tableName));
        ((QueryRunner) getExpectedQueryRunner()).execute(
                "INSERT INTO " + tableName + " " +
                        "SELECT int_col, bigint_col, varchar_col, date_col FROM test_data");

        // Get the partition information from the metadata tables
        MaterializedResult partitionInfo = computeActual(
                "SELECT file_path FROM \"" + tableName + "$files\"");
        MaterializedResult javaPartitionInfo = computeExpected(
                "SELECT file_path FROM \"" + tableName + "$files\"", ImmutableList.of());
        // Verify we have at least one file
        assertEquals(partitionInfo.getRowCount(), javaPartitionInfo.getRowCount(), "Should have same number of data file");

        // Parse the partition values from the file paths
        Set<String> bucketValues = new HashSet<>();
        Set<String> truncateValues = new HashSet<>();
        Set<String> yearValues = new HashSet<>();

        for (int i = 0; i < partitionInfo.getRowCount(); i++) {
            String filePath = (String) partitionInfo.getMaterializedRows().get(i).getField(0);
            Map<String, String> partitionValues = parsePartitionValues(filePath, 3);

            // Verify all three partition columns exist in the path
            assertTrue(partitionValues.containsKey("int_col_bucket"),
                    "Partition column int_col_bucket should exist in path: " + filePath);
            assertTrue(partitionValues.containsKey("varchar_col_trunc"),
                    "Partition column varchar_col_truncate should exist in path: " + filePath);
            assertTrue(partitionValues.containsKey("date_col_year"),
                    "Partition column date_col_year should exist in path: " + filePath);

            // Collect unique values for each partition column
            bucketValues.add(partitionValues.get("int_col_bucket"));
            truncateValues.add(partitionValues.get("varchar_col_trunc"));
            yearValues.add(partitionValues.get("date_col_year"));
        }

        // Verify bucket values are within range (0-3)
        for (String bucketValue : bucketValues) {
            if (!bucketValue.equals("null")) {
                int bucket = Integer.parseInt(bucketValue);
                assertTrue(bucket >= 0 && bucket < 4,
                        "Bucket value " + bucket + " should be between 0 and 3");
            }
        }
        assertQuerySucceeds(format(DROP_TABLE_TEMPLATE, tableName));
        ((QueryRunner) getExpectedQueryRunner()).execute(format(DROP_TABLE_TEMPLATE, tableName));
    }
}
