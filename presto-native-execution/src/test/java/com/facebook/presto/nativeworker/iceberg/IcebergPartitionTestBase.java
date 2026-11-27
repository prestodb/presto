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

import com.facebook.presto.Session;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class IcebergPartitionTestBase
        extends AbstractTestQueryFramework
{
    public static final String TEST_TABLE_PREFIX = "transform_test_";
    public static final String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS %s";

    public static final String TRANSFORM_IDENTITY = "identity";
    public static final String TRANSFORM_BUCKET = "bucket";
    public static final String TRANSFORM_TRUNCATE = "truncate";
    public static final String TRANSFORM_YEAR = "year";
    public static final String TRANSFORM_MONTH = "month";
    public static final String TRANSFORM_DAY = "day";
    public static final String TRANSFORM_HOUR = "hour";

    private static final Set<String> TEMPORAL_TYPES = ImmutableSet.of("date", "timestamp");

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
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder().build();
    }

    public Optional<TableNamePair> createPartitionedTables(String transform, String dataType, String param)
    {
        if (!isValidTransformForType(transform, dataType)) {
            return Optional.empty();
        }

        String nativeTableName = getTableName(transform, dataType) + "_native";
        String javaTableName = getTableName(transform, dataType) + "_java";
        String columnName = dataType + "_col";
        String partitioningClause = buildPartitioningClause(transform, columnName, param);

        dropTableSafely(nativeTableName);
        dropTableOnJavaRunner(javaTableName);

        String createTableSql = getCreateTableSql(nativeTableName, columnName, dataType, partitioningClause);
        assertQuerySucceeds(createTableSql);
        assertQuerySucceeds(format("INSERT INTO %s SELECT int_col, %s FROM test_data", nativeTableName, columnName));

        ((QueryRunner) getExpectedQueryRunner()).execute(
                getCreateTableSql(javaTableName, columnName, dataType, partitioningClause));

        Session legacyTimestampDisabled = Session.builder(getSession())
                .setSystemProperty("legacy_timestamp", "false")
                .build();

        ((QueryRunner) getExpectedQueryRunner()).execute(legacyTimestampDisabled,
                format("INSERT INTO %s SELECT int_col, %s FROM test_data", javaTableName, columnName));

        MaterializedResult nativeResult = computeActual(format("SELECT * FROM %s ORDER BY id", nativeTableName));
        MaterializedResult javaResult = computeExpected(format("SELECT * FROM %s ORDER BY id", javaTableName), ImmutableList.of());
        assertEquals(nativeResult, javaResult, "Data should match between native and Java tables");

        return Optional.of(new TableNamePair(nativeTableName, javaTableName));
    }

    protected void dropTableSafely(String tableName)
    {
        assertQuerySucceeds(format(DROP_TABLE_TEMPLATE, tableName));
    }

    protected void dropTableOnJavaRunner(String tableName)
    {
        try {
            ((QueryRunner) getExpectedQueryRunner()).execute(format(DROP_TABLE_TEMPLATE, tableName));
        }
        catch (Exception e) {
        }
    }

    public void verifyTransform(String transform, PartitionInfo partitionInfo)
    {
        assertTrue(partitionInfo.partitionValuesMatch(),
                format("Partition values should match between native and Java tables for transform %s", transform));
        assertTrue(partitionInfo.partitionCountsMatch(),
                format("Partition file counts should match between native and Java tables for transform %s", transform));
    }

    public void verifyPartitionTransform(String transform, String param, PartitionInfo partitionInfo)
    {
        switch (transform) {
            case TRANSFORM_BUCKET:
                verifyBucketTransform(param, partitionInfo);
                break;
            case "trunc":
            case TRANSFORM_TRUNCATE:
            case TRANSFORM_YEAR:
            case TRANSFORM_MONTH:
            case TRANSFORM_DAY:
            case TRANSFORM_HOUR:
            case TRANSFORM_IDENTITY:
                verifyTransform(transform, partitionInfo);
                break;
            default:
                fail("No specific verification for transform: " + transform);
        }
    }

    public String buildPartitioningClause(String transform, String column, String parameter)
    {
        switch (transform) {
            case TRANSFORM_IDENTITY:
                return column;
            case TRANSFORM_BUCKET:
                if (parameter == null) {
                    throw new IllegalArgumentException("Bucket transform requires a parameter");
                }
                return format("bucket(%s, %s)", column, parameter);
            case "trunc":
            case TRANSFORM_TRUNCATE:
                if (parameter == null) {
                    throw new IllegalArgumentException("Truncate transform requires a parameter");
                }
                return format("truncate(%s, %s)", column, parameter);
            case TRANSFORM_YEAR:
            case TRANSFORM_MONTH:
            case TRANSFORM_DAY:
            case TRANSFORM_HOUR:
                return format("%s(%s)", transform, column);
            default:
                throw new IllegalArgumentException("Unknown transform: " + transform);
        }
    }

    public PartitionInfo collectPartitionInfo(String transform, String nativeTableName, String javaTableName, String columnName)
    {
        MaterializedResult nativePartitionInfo = computeActual(
                format("SELECT file_path FROM \"%s$files\"", nativeTableName));

        MaterializedResult javaPartitionInfo = computeExpected(
                format("SELECT file_path FROM \"%s$files\"", javaTableName),
                ImmutableList.of());

        assertTrue(nativePartitionInfo.getRowCount() > 0, "Native runner should have at least one data file");
        assertEquals(nativePartitionInfo.getRowCount(), javaPartitionInfo.getRowCount(),
                "Native and Java tables should have the same number of data files");

        String partitionColumnName = getPartitionColumnName(columnName, transform);

        Set<String> nativeUniquePartitionValues = new HashSet<>();
        Map<String, Integer> nativePartitionValueCounts = new HashMap<>();
        collectPartitionValuesFromFiles(nativePartitionInfo, partitionColumnName,
                nativeUniquePartitionValues, nativePartitionValueCounts);

        Set<String> javaUniquePartitionValues = new HashSet<>();
        Map<String, Integer> javaPartitionValueCounts = new HashMap<>();
        collectPartitionValuesFromFiles(javaPartitionInfo, partitionColumnName,
                javaUniquePartitionValues, javaPartitionValueCounts);

        return new PartitionInfo(
                nativePartitionInfo,
                javaPartitionInfo,
                partitionColumnName,
                nativeUniquePartitionValues,
                javaUniquePartitionValues,
                nativePartitionValueCounts,
                javaPartitionValueCounts);
    }

    private void collectPartitionValuesFromFiles(MaterializedResult fileInfo, String partitionColumnName,
            Set<String> uniqueValues, Map<String, Integer> valueCounts)
    {
        for (int i = 0; i < fileInfo.getRowCount(); i++) {
            String filePath = (String) fileInfo.getMaterializedRows().get(i).getField(0);
            Map<String, String> partitionValues = parsePartitionValues(filePath, 1);

            assertTrue(partitionValues.containsKey(partitionColumnName),
                    format("Partition column %s should exist in path: %s", partitionColumnName, filePath));

            String partitionValue = partitionValues.get(partitionColumnName);
            uniqueValues.add(partitionValue);
            valueCounts.merge(partitionValue, 1, Integer::sum);
        }
    }

    public String getPartitionColumnName(String columnName, String transform)
    {
        if (TRANSFORM_IDENTITY.equals(transform)) {
            return columnName;
        }
        return columnName + "_" + transform;
    }

    public void verifyBucketTransform(String param, PartitionInfo partitionInfo)
    {
        int bucketCount = Integer.parseInt(param);
        assertTrue(partitionInfo.nativeUniquePartitionValues.size() > 0,
                "Should have at least one unique bucket value");

        for (String bucketValue : partitionInfo.nativeUniquePartitionValues) {
            if ("null".equals(bucketValue)) {
                continue;
            }
            int bucket = Integer.parseInt(bucketValue);
            assertTrue(bucket >= 0 && bucket < bucketCount,
                    format("Bucket value %d should be in range [0, %d)", bucket, bucketCount));
        }
    }

    public void verifyPartitionsMetadata(String nativeTableName, String javaTableName)
    {
        verifyPartitionsMetadata(nativeTableName, javaTableName, 1);
    }

    public void verifyPartitionsMetadata(String nativeTableName, String javaTableName, int partitionColumnCount)
    {
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

        assertTrue(nativePartitions.getRowCount() > 0,
                format("Native table %s should have partition metadata", nativeTableName));

        assertEquals(nativePartitions.getRowCount(), javaPartitions.getRowCount(),
                "Native and Java tables should have the same number of partition rows");

        for (int i = 0; i < nativePartitions.getRowCount(); i++) {
            MaterializedRow nativeRow = nativePartitions.getMaterializedRows().get(i);
            MaterializedRow javaRow = javaPartitions.getMaterializedRows().get(i);

            assertEquals(nativeRow.getField(0), javaRow.getField(0),
                    "Partition value should match between native and Java tables");
            assertEquals(nativeRow.getField(1), javaRow.getField(1),
                    "Row count should match between native and Java tables");
            assertEquals(nativeRow.getField(2), javaRow.getField(2),
                    "File count should match between native and Java tables");
        }
    }

    public String getTableName(String transform, String dataType)
    {
        return TEST_TABLE_PREFIX + transform + "_" + dataType;
    }

    protected boolean isValidTransformForType(String transform, String dataType)
    {
        switch (transform) {
            case TRANSFORM_YEAR:
            case TRANSFORM_MONTH:
            case TRANSFORM_DAY:
                return TEMPORAL_TYPES.contains(dataType);
            case TRANSFORM_HOUR:
                return "timestamp".equals(dataType);
            default:
                return true;
        }
    }

    private String getDataTypeDefinition(String dataType)
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
            case "timestamp":
                return "TIMESTAMP";
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private String getCreateTableSql(String tableName, String columnName, String dataType, String partitioningClause)
    {
        return format("CREATE TABLE %s (" +
                        "  id INTEGER, " +
                        "  %s %s" +
                        ") WITH (format = 'PARQUET', partitioning = ARRAY['%s'])",
                tableName, columnName, getDataTypeDefinition(dataType), partitioningClause);
    }

    protected static Map<String, String> parsePartitionValues(String filePath, int partitionColumnCount)
    {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        String[] pathParts = filePath.split("/");

        int startIndex = pathParts.length - partitionColumnCount - 1;
        if (startIndex < 0) {
            throw new IllegalArgumentException(format(
                    "File path does not contain enough parts for %d partition columns: %s",
                    partitionColumnCount, filePath));
        }

        for (int i = 0; i < partitionColumnCount; i++) {
            String partitionPart = pathParts[startIndex + i];
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

        public boolean partitionValuesMatch()
        {
            return nativeUniquePartitionValues.equals(javaUniquePartitionValues);
        }

        public boolean partitionCountsMatch()
        {
            return nativePartitionValueCounts.equals(javaPartitionValueCounts);
        }
    }

    public static class TableNamePair
    {
        private final String nativeTableName;
        private final String javaTableName;

        public TableNamePair(String nativeTableName, String javaTableName)
        {
            this.nativeTableName = nativeTableName;
            this.javaTableName = javaTableName;
        }

        public String getNativeTableName()
        {
            return nativeTableName;
        }

        public String getJavaTableName()
        {
            return javaTableName;
        }
    }
}
