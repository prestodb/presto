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

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestTpchPartitionTransforms
        extends IcebergPartitionTestBase
{
    private static final String TEST_TABLE_PREFIX = "partition_test_";
    private static final String sourceTableName = "lineitem";
    private static final String sourceTableSchema = "iceberg.tpch";
    private static final String CREATE_LINEITEM_TABLE_TEMPLATE = "CREATE TABLE %s (" +
            "  orderkey BIGINT, " +
            "  partkey BIGINT, " +
            "  suppkey BIGINT, " +
            "  linenumber INTEGER, " +
            "  quantity DECIMAL(12,2), " +
            "  extendedprice DECIMAL(12,2), " +
            "  discount DECIMAL(12,2), " +
            "  tax DECIMAL(12,2), " +
            "  returnflag VARCHAR, " +
            "  linestatus VARCHAR, " +
            "  shipdate DATE, " +
            "  commitdate DATE, " +
            "  receiptdate DATE, " +
            "  shipinstruct VARCHAR, " +
            "  shipmode VARCHAR, " +
            "  comment VARCHAR" +
            ") WITH (format = 'PARQUET', partitioning = ARRAY['%s'])";
    private static final String INSERT_LINEITEM_TEMPLATE = "INSERT INTO %s SELECT orderkey, partkey, suppkey, linenumber," +
            "CAST(quantity AS DECIMAL(12,2)), CAST(extendedprice AS DECIMAL(12,2))," +
            "CAST(discount AS DECIMAL(12,2)), CAST(tax AS DECIMAL(12,2))," +
            "CAST(returnflag AS VARCHAR(1)), CAST(linestatus AS VARCHAR(1))," +
            "CAST(shipdate AS DATE), CAST(commitdate AS DATE), CAST(receiptdate as DATE)," +
            "CAST(shipinstruct AS VARCHAR(25)), CAST(shipmode AS VARCHAR(10))," +
            "CAST(comment AS VARCHAR(44)) FROM %s.%s";

    private static Object[][] getPartitionTransformTestParameters()
    {
        return new Object[][] {
                {"year", "", "shipdate"},
                {"bucket", "8", "orderkey"},
                {"bucket", "4", "shipdate"},
                {"bucket", "4", "quantity"},
                {"bucket", "10", "tax"},
                {"bucket", "10", "linenumber"}
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder().setUseThrift(true).build();
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder().build();
        NativeQueryRunnerUtils.createAllIcebergTables(javaQueryRunner);
        return javaQueryRunner;
    }

    private String[] createPartitionedLineitemTables(String targetTableName, String partitioningClause)
    {
        String nativeTableName = targetTableName + "_native";
        String javaTableName = targetTableName + "_java";

        ((QueryRunner) getExpectedQueryRunner()).execute(format(DROP_TABLE_TEMPLATE, javaTableName));
        ((QueryRunner) getExpectedQueryRunner()).execute(format(CREATE_LINEITEM_TABLE_TEMPLATE,
                javaTableName, partitioningClause));
        ((QueryRunner) getExpectedQueryRunner()).execute(format(INSERT_LINEITEM_TEMPLATE, javaTableName, sourceTableSchema, sourceTableName));

        assertQuerySucceeds(format(DROP_TABLE_TEMPLATE, nativeTableName));
        assertQuerySucceeds(format(
                CREATE_LINEITEM_TABLE_TEMPLATE,
                nativeTableName, partitioningClause));
        assertQuerySucceeds(format(INSERT_LINEITEM_TEMPLATE, nativeTableName, sourceTableSchema, sourceTableName));

        MaterializedResult nativeResult = computeActual(format("SELECT count(*) FROM %s", nativeTableName));
        MaterializedResult javaResult = computeExpected(format("SELECT count(*) FROM %s", javaTableName), ImmutableList.of());
        assertEquals(nativeResult, javaResult, "Row count should match between native and Java tables");

        return new String[] {nativeTableName, javaTableName};
    }

    @Test(dataProvider = "partitionTransformTestParameters")
    public void testPartitionTransform(String transform, String parameter, String column)
    {
        String partitioningClause = buildPartitioningClause(transform, column, parameter);
        String[] tableNames = createPartitionedLineitemTables(sourceTableName, partitioningClause);
        String nativeTableName = tableNames[0];
        String javaTableName = tableNames[1];

        try {
            String partitionColumnName = getPartitionColumnName(column, transform);
            MaterializedResult nativePartitions = computeActual(
                    format("SELECT DISTINCT %s FROM \"%s$partitions\"", partitionColumnName, nativeTableName));
            MaterializedResult javaPartitions = computeExpected(
                    format("SELECT DISTINCT %s FROM \"%s$partitions\"", partitionColumnName, javaTableName),
                    ImmutableList.of());

            assertTrue(nativePartitions.getRowCount() > 0, "Should have partitions in native table");
            assertEquals(nativePartitions.getRowCount(), javaPartitions.getRowCount(),
                    "Native and Java tables should have the same number of partitions");

            assertQuery(format("SELECT * FROM %s ORDER BY orderkey", nativeTableName), format("SELECT * FROM %s ORDER BY orderkey", javaTableName));

            PartitionInfo partitionInfo = collectPartitionInfo(transform,
                    nativeTableName, javaTableName, column);

            verifyPartitionTransform(transform, parameter, partitionInfo);

            verifyPartitionsMetadata(nativeTableName, javaTableName);
        }
        finally {
            cleanupTables(nativeTableName, javaTableName);
        }
    }

    @DataProvider(name = "partitionTransformTestParameters")
    public Object[][] partitionTransformTestParameters()
    {
        return getPartitionTransformTestParameters();
    }

    private void cleanupTables(String nativeTableName, String javaTableName)
    {
        try {
            assertQuerySucceeds(format("DROP TABLE IF EXISTS %s", nativeTableName));
        }
        catch (Exception e) {
        }

        try {
            ((QueryRunner) getExpectedQueryRunner()).execute(
                    format("DROP TABLE IF EXISTS %s", javaTableName));
        }
        catch (Exception e) {
        }
    }

    @Test(enabled = false)
    public void testMultiplePartitionTransforms()
    {
        String targetTableName = TEST_TABLE_PREFIX + "lineitem_multiple_transforms";
        String nativeTableName = targetTableName + "_native";
        String javaTableName = targetTableName + "_java";

        assertQuerySucceeds(format(DROP_TABLE_TEMPLATE, nativeTableName));
        assertQuerySucceeds(format(
                "CREATE TABLE %s (" +
                        "  orderkey BIGINT, " +
                        "  partkey BIGINT, " +
                        "  suppkey BIGINT, " +
                        "  linenumber INTEGER, " +
                        "  quantity DOUBLE, " +
                        "  extendedprice DOUBLE, " +
                        "  discount DOUBLE, " +
                        "  tax DOUBLE, " +
                        "  returnflag VARCHAR, " +
                        "  linestatus VARCHAR, " +
                        "  shipdate DATE, " +
                        "  commitdate DATE, " +
                        "  receiptdate DATE, " +
                        "  shipinstruct VARCHAR, " +
                        "  shipmode VARCHAR, " +
                        "  comment VARCHAR" +
                        ") WITH (format = 'PARQUET', partitioning = ARRAY['bucket(orderkey, 4)', 'truncate(shipmode, 1)', 'month(shipdate)'])",
                nativeTableName));

        assertQuerySucceeds(format(INSERT_LINEITEM_TEMPLATE, nativeTableName, sourceTableSchema, sourceTableName));

        ((QueryRunner) getExpectedQueryRunner()).execute(format(
                "CREATE TABLE %s WITH (format = 'PARQUET', partitioning = ARRAY['bucket(orderkey, 4)', 'truncate(shipmode, 1)', 'month(shipdate)']) AS " +
                        "SELECT * FROM %s",
                javaTableName, sourceTableName));

        MaterializedResult nativeResult = computeActual(format("SELECT count(*) FROM %s", nativeTableName));
        MaterializedResult javaResult = computeExpected(format("SELECT count(*) FROM %s", javaTableName), ImmutableList.of());
        assertEquals(nativeResult, javaResult, "Row count should match between native and Java tables");

        MaterializedResult nativePartitions = computeActual(format(
                "SELECT DISTINCT orderkey_bucket, shipmode_trunc, shipdate_month FROM \"%s$partitions\"",
                nativeTableName));
        assertTrue(nativePartitions.getRowCount() > 0, "Should have partitions");

        String query = format(
                "SELECT count(*) FROM %s WHERE orderkey %% 4 = 0 AND shipmode LIKE 'A%%'",
                nativeTableName);
        computeActual(query);

        verifyPartitionsMetadata(nativeTableName, javaTableName);
    }
}
