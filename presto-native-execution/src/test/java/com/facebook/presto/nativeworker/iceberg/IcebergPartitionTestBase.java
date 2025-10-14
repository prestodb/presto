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

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Helper class for testing Iceberg partition transforms in Presto
 */
public class IcebergPartitionTestBase
        extends AbstractTestQueryFramework
{
    public static final String TEST_TABLE_PREFIX = "partition_transform_test_";
    public static final String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS %s";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder().build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .build();
    }

    /**
     * Creates tables with the specified partition transform and data type
     */
    public String[] createPartitionedTables(String transform, String dataType, String param)
    {
        if (!isValidTransformForType(transform, dataType)) {
            return null;
        }

        String nativeTableName = getTableName(transform, dataType) + "_native";
        String javaTableName = getTableName(transform, dataType) + "_java";
        String columnName = dataType + "_col";
        String partitioningClause = buildPartitioningClause(transform, columnName, param);

        assertQuerySucceeds(format(DROP_TABLE_TEMPLATE, nativeTableName));
        ((QueryRunner) getExpectedQueryRunner()).execute(format(DROP_TABLE_TEMPLATE, javaTableName));
        String createTableSql = getCreateTableSql(nativeTableName, columnName, dataType, partitioningClause);
        assertQuerySucceeds(createTableSql);

        assertQuerySucceeds(
                format("INSERT INTO %s SELECT int_col, %s FROM test_data",
                        nativeTableName, columnName));

        ((QueryRunner) getExpectedQueryRunner()).execute(
                getCreateTableSql(javaTableName, columnName, dataType, partitioningClause));

        MaterializedResult jrs = ((QueryRunner) getExpectedQueryRunner()).execute(
                format("INSERT INTO %s SELECT int_col, %s FROM test_data",
                        javaTableName, columnName));

        MaterializedResult nativeResult = computeActual(
                format("SELECT * FROM %s ORDER BY id", nativeTableName));
        MaterializedResult javaResult = computeExpected(
                format("SELECT * FROM %s ORDER BY id", javaTableName),
                ImmutableList.of());
        assertEquals(nativeResult, javaResult, "Data should match between native and Java tables");

        return new String[] {nativeTableName, javaTableName};
    }

    public void verifyTransform(String transform, String nativeTableName, String javaTableName,
            String columnName, PartitionInfo partitionInfo)
    {
        for (String partitionValue : partitionInfo.nativeUniquePartitionValues) {
            String partitionPath;
            if (transform.equals("identity")) {
                partitionPath = columnName + "=" + partitionValue;
            }
            else {
                partitionPath = columnName + "_" + transform + "=" + partitionValue;
            }

            verifyFileMetadata(nativeTableName, javaTableName, partitionPath);
        }
    }

    public void verifyPartitionTransform(String transform, String param, String nativeTableName,
            String javaTableName, String partitionColumn, PartitionInfo partitionInfo)
    {
        switch (transform) {
            case "bucket":
                verifyBucketTransform(param, nativeTableName, javaTableName,
                        partitionColumn, partitionInfo);
                break;
            case "trunc":
            case "year":
            case "month":
            case "day":
            case "identity":
                verifyTransform(transform, nativeTableName, javaTableName, partitionColumn, partitionInfo);
                break;
            default:
                fail("No specific verification for transform: " + transform);
        }
    }

    public String buildPartitioningClause(String transform, String column, String parameter)
    {
        if ("identity".equals(transform)) {
            return column;
        }
        else if ("bucket".equals(transform) && parameter != null) {
            return format("%s(%s, %s)", transform, column, parameter);
        }
        else if ("trunc".equals(transform) && parameter != null) {
            return format("%s(%s, %s)", "truncate", column, parameter);
        }
        else {
            return format("%s(%s)", transform, column);
        }
    }

    /**
     * Collects partition information from native and Java tables
     */
    public PartitionInfo collectPartitionInfo(String transform, String nativeTableName, String javaTableName, String columnName)
    {
        MaterializedResult nativePartitionInfo = computeActual(
                format("SELECT file_path FROM \"%s$files\"", nativeTableName));

        MaterializedResult javaPartitionInfo = computeExpected(
                format("SELECT file_path FROM \"%s$files\"", javaTableName),
                ImmutableList.of());

        assertTrue(nativePartitionInfo.getRowCount() > 0, "Native runner should have at least one data file");
        assertEquals(nativePartitionInfo.getRowCount(), javaPartitionInfo.getRowCount(), "They should have same number of data files");

        String partitionColumnName;
        if (transform.equals("identity")) {
            partitionColumnName = columnName;
        }
        else {
            partitionColumnName = columnName + "_" + transform;
        }

        Set<String> nativeUniquePartitionValues = new HashSet<>();
        Map<String, Integer> nativePartitionValueCounts = new HashMap<>();

        for (int i = 0; i < nativePartitionInfo.getRowCount(); i++) {
            String filePath = (String) nativePartitionInfo.getMaterializedRows().get(i).getField(0);
            Map<String, String> partitionValues = parsePartitionValues(filePath, 1);

            assertTrue(partitionValues.containsKey(partitionColumnName),
                    format("Partition column %s should exist in path: %s", partitionColumnName, filePath));

            String partitionValue = partitionValues.get(partitionColumnName);
            nativeUniquePartitionValues.add(partitionValue);

            nativePartitionValueCounts.put(partitionValue, nativePartitionValueCounts.getOrDefault(partitionValue, 0) + 1);
        }

        Set<String> javaUniquePartitionValues = new HashSet<>();
        Map<String, Integer> javaPartitionValueCounts = new HashMap<>();

        for (int i = 0; i < javaPartitionInfo.getRowCount(); i++) {
            String filePath = (String) javaPartitionInfo.getMaterializedRows().get(i).getField(0);
            Map<String, String> partitionValues = parsePartitionValues(filePath, 1);

            assertTrue(partitionValues.containsKey(partitionColumnName),
                    format("Partition column %s should exist in path: %s", partitionColumnName, filePath));

            String partitionValue = partitionValues.get(partitionColumnName);
            javaUniquePartitionValues.add(partitionValue);

            javaPartitionValueCounts.put(partitionValue, javaPartitionValueCounts.getOrDefault(partitionValue, 0) + 1);
        }

        return new PartitionInfo(
                nativePartitionInfo,
                javaPartitionInfo,
                partitionColumnName,
                nativeUniquePartitionValues,
                javaUniquePartitionValues,
                nativePartitionValueCounts,
                javaPartitionValueCounts);
    }

    /**
     * Verifies bucket transform
     */
    public void verifyBucketTransform(String param, String nativeTableName, String javaTableName,
            String columnName, PartitionInfo partitionInfo)
    {
        int bucketCount = Integer.parseInt(param);
        assertTrue(partitionInfo.nativeUniquePartitionValues.size() > 0,
                "Should have at least one unique bucket value");

        for (String bucketValue : partitionInfo.nativeUniquePartitionValues) {
            if (bucketValue.equals("null")) {
                continue;
            }
            int bucket = Integer.parseInt(bucketValue);
            assertTrue(bucket >= 0 && bucket < bucketCount,
                    format("Bucket value %d should be between 0 and %d", bucket, (bucketCount - 1)));
        }

        for (String bucketValue : partitionInfo.nativeUniquePartitionValues) {
            String partitionPath = columnName + "_bucket=" + bucketValue;
            verifyFileMetadata(nativeTableName, javaTableName, partitionPath);
        }
    }

    /**
     * Verifies file metadata between native and Java tables
     */
    public void verifyFileMetadata(String nativeTableName, String javaTableName,
            String nativePartitionPath)
    {
        MaterializedResult nativeFiles = computeActual(
                format("SELECT file_path, record_count FROM \"%s$files\" WHERE file_path LIKE '%%/%s/%%'",
                        nativeTableName, nativePartitionPath));

        MaterializedResult javaFiles = computeExpected(
                format("SELECT file_path, record_count FROM \"%s$files\" WHERE file_path LIKE '%%/%s/%%'",
                        javaTableName, nativePartitionPath),
                ImmutableList.of());

        assertTrue(nativeFiles.getRowCount() > 0,
                format("Should have at least one file in partition %s", nativePartitionPath));

        // Verify metadata fields for each file
        for (int i = 0; i < nativeFiles.getRowCount(); i++) {
            MaterializedRow nativeRow = nativeFiles.getMaterializedRows().get(i);

            // Find matching Java file with same record count
            MaterializedRow matchingJavaRow = findMatchingJavaFile(nativeRow, javaFiles);
            assertNotNull(matchingJavaRow, format("Could not find matching Java file for native file with record count %d", nativeRow.getField(1)));
        }
    }

    /**
     * Finds a matching Java file with the same record count
     */
    public MaterializedRow findMatchingJavaFile(MaterializedRow nativeRow, MaterializedResult javaFiles)
    {
        String nativeFilePath = (String) nativeRow.getField(0);
        long nativeRecordCount = (long) nativeRow.getField(1);

        // Extract partition folder from native file path
        Map<String, String> nativePartitionFolder = parsePartitionValues(nativeFilePath, 1);

        for (MaterializedRow javaRow : javaFiles.getMaterializedRows()) {
            String javaFilePath = (String) javaRow.getField(0);
            long javaRecordCount = (long) javaRow.getField(1);
            // Extract partition folder from Java file path
            Map<String, String> javaPartitionFolder = parsePartitionValues(javaFilePath, 1);
            // Check if record counts match
            if (nativeRecordCount == javaRecordCount && nativePartitionFolder.equals(javaPartitionFolder)) {
                // For other transforms, just check if record counts match
                return javaRow;
            }
        }
        return null;
    }

    public void verifyPartitionsMetadata(String nativeTableName, String javaTableName)
    {
        int partitionColumnCount = 1;
        StringBuilder orderByClause = new StringBuilder("ORDER BY ");
        for (int i = 0; i < partitionColumnCount; i++) {
            if (i > 0) {
                orderByClause.append(", ");
            }
            orderByClause.append(i + 1);
        }

        MaterializedResult nativePartitions = computeActual(
                format("SELECT * FROM \"%s$partitions\" %s", nativeTableName, orderByClause));

        MaterializedResult javaPartitions = computeExpected(
                format("SELECT * FROM \"%s$partitions\" %s", javaTableName, orderByClause),
                ImmutableList.of());

        // Verify we have partition metadata
        assertTrue(nativePartitions.getRowCount() > 0,
                format("Native table %s should have partition metadata", nativeTableName));
        assertTrue(javaPartitions.getRowCount() > 0,
                format("Java table %s should have partition metadata", javaTableName));

        // Verify row count matches
        assertEquals(nativePartitions.getRowCount(), javaPartitions.getRowCount(),
                "Native and Java tables should have the same number of partition rows");

        // For each partition row, verify the metadata
        for (int i = 0; i < nativePartitions.getRowCount(); i++) {
            MaterializedRow nativeRow = nativePartitions.getMaterializedRows().get(i);
            MaterializedRow javaRow = javaPartitions.getMaterializedRows().get(i);

            // Verify row_count
            assertEquals(nativeRow.getField(1), javaRow.getField(1),
                    "row_count should match between native and Java tables");

            // Verify file_count
            assertEquals(nativeRow.getField(2), javaRow.getField(2),
                    "file_count should match between native and Java tables");

            // Verify column statistics for each column
            /*
            for (int j = 3; j < nativeRow.getFieldCount(); j++) {
                Object nativeStats = nativeRow.getField(j);
                Object javaStats = javaRow.getField(j);

                if (nativeStats == null && javaStats == null) {
                    continue; // Both null, considered equal
                }

                assertTrue(nativeStats != null && javaStats != null,
                        format("Column statistics at index %d should both be non-null", j));

                // Parse the statistics maps
                Map<String, String> nativeMap = parseMap(nativeStats.toString());
                Map<String, String> javaMap = parseMap(javaStats.toString());

                // Verify the maps have the same keys
                assertEquals(nativeMap.keySet(), javaMap.keySet(),
                        format("Column statistics keys at index %d should match", j));

                // Verify null_count
                assertEquals(nativeMap.get("nan_count"), javaMap.get("null_count"),
                        format("null_count for column at index %d should match", j));

                // Verify min value
                assertEquals(nativeMap.get("min"), javaMap.get("min"),
                        format("min value for column at index %d should match", j));

                // Verify max value
                assertEquals(nativeMap.get("max"), javaMap.get("max"),
                        format("max value for column at index %d should match", j));
            }

             */
        }
    }

    public String getTableName(String transform, String dataType)
    {
        return TEST_TABLE_PREFIX + transform + "_" + dataType;
    }

    public boolean isValidTransformForType(String transform, String dataType)
    {
        // Not all transforms are valid for all data types
        if (transform.equals("year") || transform.equals("month") || transform.equals("day")) {
            return dataType.equals("date");
        }
        return true;
    }

    public String getDataTypeDefinition(String dataType)
    {
        switch (dataType) {
            case "int":
                return "INTEGER";
            case "bigint":
                return "BIGINT";
            case "varchar":
                return "VARCHAR";
            case "varbinary":
                return "VARBINARY";
            case "date":
                return "DATE";
            case "decimal":
                return "DECIMAL(18, 6)";
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    /**
     * Gets the create table SQL for a table
     */
    public String getCreateTableSql(String tableName, String columnName, String dataType, String partitioningClause)
    {
        return format("CREATE TABLE %s (" +
                        "  id INTEGER, " +
                        "  %s %s" +
                        ") WITH (format = 'PARQUET', partitioning = ARRAY['%s'])",
                tableName, columnName, getDataTypeDefinition(dataType), partitioningClause);
    }

    public static Map<String, String> parsePartitionValues(String filePath, int partitionColumnCount)
    {
        Map<String, String> partitionValues = new LinkedHashMap<>();

        // Split the path by '/'
        String[] pathParts = filePath.split("/");

        // The partition directories are before the file name
        // Start from the end and go backwards by partitionColumnCount + 1 (to skip the filename)
        int startIndex = pathParts.length - partitionColumnCount - 1;
        if (startIndex < 0) {
            throw new IllegalArgumentException("File path does not contain enough parts for " +
                    partitionColumnCount + " partition columns: " + filePath);
        }

        // Extract each partition key-value pair
        for (int i = 0; i < partitionColumnCount; i++) {
            String partitionPart = pathParts[startIndex + i];

            // Parse the key=value format
            int equalsPos = partitionPart.indexOf('=');
            if (equalsPos <= 0 || equalsPos == partitionPart.length() - 1) {
                throw new IllegalArgumentException("Invalid partition format in path: " + partitionPart);
            }

            String key = partitionPart.substring(0, equalsPos);
            String value = partitionPart.substring(equalsPos + 1);

            partitionValues.put(key, value);
        }

        return partitionValues;
    }

    /**
     * Class to hold partition information
     */
    public static class PartitionInfo
    {
        public final MaterializedResult nativePartitionInfo;
        public final MaterializedResult javaPartitionInfo;
        public final String partitionColumnName;
        public final Set<String> nativeUniquePartitionValues;
        public final Set<String> javaUniquePartitionValues;
        public final Map<String, Integer> nativePartitionValueCounts;
        public final Map<String, Integer> javaPartitionValueCounts;

        public PartitionInfo(
                MaterializedResult nativePartitionInfo,
                MaterializedResult javaPartitionInfo,
                String partitionColumnName,
                Set<String> nativeUniquePartitionValues,
                Set<String> javaUniquePartitionValues,
                Map<String, Integer> nativePartitionValueCounts,
                Map<String, Integer> javaPartitionValueCounts)
        {
            this.nativePartitionInfo = nativePartitionInfo;
            this.javaPartitionInfo = javaPartitionInfo;
            this.partitionColumnName = partitionColumnName;
            this.nativeUniquePartitionValues = nativeUniquePartitionValues;
            this.javaUniquePartitionValues = javaUniquePartitionValues;
            this.nativePartitionValueCounts = nativePartitionValueCounts;
            this.javaPartitionValueCounts = javaPartitionValueCounts;
        }
    }
}
