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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestPartitionTransforms
        extends IcebergPartitionTestBase
{
    private static final String CREATE_MULTIPLE_PARTITION_TABLE_TEMPLATE = "CREATE TABLE %s (" +
            "  int_col INTEGER, " +
            "  bigint_col BIGINT, " +
            "  varchar_col VARCHAR, " +
            "  date_col DATE" +
            ") WITH (format = 'PARQUET', partitioning = ARRAY['bucket(date_col, 4)', 'date_col', 'year(date_col)'])";

    private static final String[] ALL_DATA_TYPES = {"int", "bigint", "varchar", "varbinary", "date", "timestamp", "decimal"};
    private static final String[] TEMPORAL_DATA_TYPES = {"date", "timestamp"};

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
                        "  timestamp_col TIMESTAMP, " +
                        "  decimal_col DECIMAL(18, 6)" +
                        ")");

        assertQuerySucceeds(
                "INSERT INTO test_data VALUES " +
                        "  (1, 1000, 'apple', X'01020304', DATE '2023-01-15', TIMESTAMP '2023-01-15 10:30:00', DECIMAL '123.456'), " +
                        "  (2, 2000, 'banana', X'05060708', DATE '2023-02-20', TIMESTAMP '2023-02-20 14:45:30', DECIMAL '234.567'), " +
                        "  (3, 3000, 'cherry', X'090A0B0C', DATE '2023-03-25', TIMESTAMP '2023-03-25 09:15:45', DECIMAL '345.678'), " +
                        "  (4, 4000, 'date', X'0D0E0F10', DATE '2023-04-30', TIMESTAMP '2023-04-30 16:20:10', DECIMAL '456.789'), " +
                        "  (NULL, NULL, NULL, NULL, NULL, NULL, NULL), " +
                        "  (5, 5000, 'elderberry', X'11121314', DATE '2023-05-05', TIMESTAMP '2023-05-05 11:55:25', DECIMAL '567.890'), " +
                        "  (10, 10000, 'fig', X'15161718', DATE '2023-06-10', TIMESTAMP '2023-06-10 13:40:50', DECIMAL '678.901'), " +
                        "  (NULL, NULL, NULL, NULL, NULL, NULL, NULL), " +
                        "  (20, 20000, 'grape', X'191A1B1C', DATE '2023-07-15', TIMESTAMP '2023-07-15 08:25:35', DECIMAL '789.012'), " +
                        "  (30, 30000, 'honeydew', X'1D1E1F20', DATE '2023-08-20', TIMESTAMP '2023-08-20 17:10:15', DECIMAL '0.000001'), " +
                        "  (40, 40000, 'imbe', X'21222324', DATE '2023-09-25', TIMESTAMP '2023-09-25 12:05:40', DECIMAL '1.0'), " +
                        "  (NULL, NULL, NULL, NULL, NULL, NULL, NULL), " +
                        "  (50, 50000, 'jackfruit', X'25262728', DATE '2023-10-30', TIMESTAMP '2023-10-30 15:50:20', DECIMAL '1.00000'), " +
                        "  (NULL, NULL, NULL, NULL, NULL, NULL, NULL), " +
                        "  (NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        dropTableSafely("test_data");

        String[] transforms = {"identity", "bucket", "trunc", "year", "month", "day", "hour"};
        for (String transform : transforms) {
            for (String dataType : ALL_DATA_TYPES) {
                if (isValidTransformForType(transform, dataType)) {
                    String nativeTableName = getTableName(transform, dataType) + "_native";
                    String javaTableName = getTableName(transform, dataType) + "_java";
                    dropTableSafely(nativeTableName);
                    dropTableOnJavaRunner(javaTableName);
                }
            }
        }

        dropTableSafely(TEST_TABLE_PREFIX + "multiple");
    }

    @DataProvider(name = "identityTransformData")
    public Object[][] identityTransformData()
    {
        List<Object[]> data = new ArrayList<>();
        for (String dataType : ALL_DATA_TYPES) {
            data.add(new Object[] {dataType});
        }
        return data.toArray(new Object[0][]);
    }

    @DataProvider(name = "bucketTransformData")
    public Object[][] bucketTransformData()
    {
        return new Object[][] {
                {"int", "4"},
                {"bigint", "4"},
                {"varchar", "4"},
                {"varbinary", "4"},
                {"decimal", "4"},
                {"date", "4"},
        };
    }

    @DataProvider(name = "truncateTransformData")
    public Object[][] truncateTransformData()
    {
        return new Object[][] {
                {"int", "10"},
                {"bigint", "1000"},
                {"varchar", "3"},
                {"varbinary", "2"},
                {"decimal", "100"},
        };
    }

    @DataProvider(name = "temporalTransformData")
    public Object[][] temporalTransformData()
    {
        return new Object[][] {
                {"year", "date"},
                {"year", "timestamp"},
                {"month", "date"},
                {"month", "timestamp"},
                {"day", "date"},
                {"day", "timestamp"},
        };
    }

    @Test(dataProvider = "identityTransformData")
    public void testIdentityTransform(String dataType)
    {
        testPartitionTransform(TRANSFORM_IDENTITY, dataType, null);
    }

    @Test(dataProvider = "bucketTransformData")
    public void testBucketTransform(String dataType, String bucketCount)
    {
        testPartitionTransform(TRANSFORM_BUCKET, dataType, bucketCount);
    }

    @Test(enabled = false)
    public void testBucketTransformTimestamp()
    {
        testPartitionTransform(TRANSFORM_BUCKET, "timestamp", "3");
    }

    @Test(dataProvider = "truncateTransformData")
    public void testTruncateTransform(String dataType, String truncateWidth)
    {
        testPartitionTransform("trunc", dataType, truncateWidth);
    }

    @Test(dataProvider = "temporalTransformData")
    public void testTemporalTransform(String transform, String dataType)
    {
        testPartitionTransform(transform, dataType, null);
    }

    @Test
    public void testHourTransformTimestamp()
    {
        testPartitionTransform(TRANSFORM_HOUR, "timestamp", null);
    }

    private void testPartitionTransform(String transform, String dataType, String param)
    {
        Optional<TableNamePair> maybeTableNames = createPartitionedTables(transform, dataType, param);

        if (!maybeTableNames.isPresent()) {
            return;
        }

        TableNamePair tableNames = maybeTableNames.get();

        String columnName = dataType + "_col";

        PartitionInfo partitionInfo = collectPartitionInfo(transform, tableNames.getNativeTableName(), tableNames.getJavaTableName(), columnName);

        assertTrue(partitionInfo.partitionValuesMatch(),
                format("Native and Java runners should generate the same partition values for %s transform on %s. " +
                                "Native: %s, Java: %s",
                        transform, dataType,
                        partitionInfo.nativeUniquePartitionValues,
                        partitionInfo.javaUniquePartitionValues));

        assertQuery(format("SELECT * FROM %s", tableNames.getNativeTableName()), format("SELECT * FROM %s", tableNames.getJavaTableName()));
        assertQuery(format("SELECT * FROM %s", tableNames.getJavaTableName()), format("SELECT * FROM %s", tableNames.getNativeTableName()));

        verifyPartitionTransform(transform, param, partitionInfo);
        verifyPartitionsMetadata(tableNames.getNativeTableName(), tableNames.getJavaTableName());
    }

    @Test
    public void testMultiplePartitionTransforms()
    {
        String tableName = TEST_TABLE_PREFIX + "multiple";

        dropTableSafely(tableName);
        assertQuerySucceeds(format(CREATE_MULTIPLE_PARTITION_TABLE_TEMPLATE, tableName));
        assertQuerySucceeds("INSERT INTO " + tableName +
                " SELECT int_col, bigint_col, varchar_col, date_col FROM test_data");

        MaterializedResult result = computeActual("SELECT * FROM " + tableName + " ORDER BY int_col");
        MaterializedResult expected = computeActual(
                "SELECT int_col, bigint_col, varchar_col, date_col FROM test_data ORDER BY int_col");
        assertEquals(result, expected, "Data should match after insertion");

        dropTableOnJavaRunner(tableName);
        ((QueryRunner) getExpectedQueryRunner()).execute(format(CREATE_MULTIPLE_PARTITION_TABLE_TEMPLATE, tableName));
        ((QueryRunner) getExpectedQueryRunner()).execute("INSERT INTO " + tableName +
                " SELECT int_col, bigint_col, varchar_col, date_col FROM test_data");

        MaterializedResult nativeFileInfo = computeActual("SELECT file_path FROM \"" + tableName + "$files\"");
        MaterializedResult javaFileInfo = computeExpected(
                "SELECT file_path FROM \"" + tableName + "$files\"", ImmutableList.of());
        assertEquals(nativeFileInfo.getRowCount(), javaFileInfo.getRowCount(),
                "Native and Java tables should have the same number of data files");

        verifyMultiplePartitionStructure(nativeFileInfo, javaFileInfo);

        dropTableSafely(tableName);
        dropTableOnJavaRunner(tableName);
    }

    private void verifyMultiplePartitionStructure(MaterializedResult nativeFileInfo, MaterializedResult javaFileInfo)
    {
        Set<String> bucketValues = new HashSet<>();
        Set<String> dateValues = new HashSet<>();
        Set<String> yearValues = new HashSet<>();

        for (int i = 0; i < nativeFileInfo.getRowCount(); i++) {
            String nativeFilePath = (String) nativeFileInfo.getMaterializedRows().get(i).getField(0);
            String javaFilePath = (String) javaFileInfo.getMaterializedRows().get(i).getField(0);

            Map<String, String> nativePartitionValues = parsePartitionValues(nativeFilePath, 3);
            Map<String, String> javaPartitionValues = parsePartitionValues(javaFilePath, 3);

            assertEquals(nativePartitionValues, javaPartitionValues,
                    "Partition values should match between native and Java tables");

            assertTrue(nativePartitionValues.containsKey("date_col_bucket"),
                    "Partition column date_col_bucket should exist in path: " + nativeFilePath);
            assertTrue(nativePartitionValues.containsKey("date_col"),
                    "Partition column date_col should exist in path: " + nativeFilePath);
            assertTrue(nativePartitionValues.containsKey("date_col_year"),
                    "Partition column date_col_year should exist in path: " + nativeFilePath);

            bucketValues.add(nativePartitionValues.get("date_col_bucket"));
            dateValues.add(nativePartitionValues.get("date_col"));
            yearValues.add(nativePartitionValues.get("date_col_year"));
        }

        for (String bucketValue : bucketValues) {
            if (!"null".equals(bucketValue)) {
                int bucket = Integer.parseInt(bucketValue);
                assertTrue(bucket >= 0 && bucket < 4,
                        format("Bucket value %d should be in range [0, 4)", bucket));
            }
        }
    }
}
