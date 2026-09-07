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

import com.facebook.presto.Session;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.iceberg.IcebergColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoNativeIcebergGeneralQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected void createTables()
    {
        createTestTables();
    }

    private void createTestTables()
    {
        QueryRunner javaQueryRunner = ((QueryRunner) getExpectedQueryRunner());

        javaQueryRunner.execute("DROP TABLE IF EXISTS test_hidden_columns");
        javaQueryRunner.execute("CREATE TABLE test_hidden_columns AS SELECT * FROM tpch.tiny.region WHERE regionkey=0");
        javaQueryRunner.execute("INSERT INTO test_hidden_columns SELECT * FROM tpch.tiny.region WHERE regionkey=1");

        javaQueryRunner.execute("DROP TABLE IF EXISTS ice_table_partitioned");
        javaQueryRunner.execute("CREATE TABLE ice_table_partitioned(c1 INT, ds DATE) WITH (partitioning = ARRAY['ds'])");
        javaQueryRunner.execute("INSERT INTO ice_table_partitioned VALUES(1, date'2022-04-09'), (2, date'2022-03-18'), (3, date'1993-01-01')");

        javaQueryRunner.execute("DROP TABLE IF EXISTS ice_table");
        javaQueryRunner.execute("CREATE TABLE ice_table(c1 INT, ds DATE)");
        javaQueryRunner.execute("INSERT INTO ice_table VALUES(1, date'2022-04-09'), (2, date'2022-03-18'), (3, date'1993-01-01')");

        javaQueryRunner.execute("DROP TABLE IF EXISTS test_analyze");
        javaQueryRunner.execute("CREATE TABLE test_analyze(i int)");
        javaQueryRunner.execute("INSERT INTO test_analyze VALUES 1, 2, 3, 4, 5");

        javaQueryRunner.execute("DROP TABLE IF EXISTS test_nested_column_pushdown");
        javaQueryRunner.execute("CREATE TABLE test_nested_column_pushdown(event_id VARCHAR, statisticsinformation ROW(processingdate VARCHAR, region VARCHAR))");
        javaQueryRunner.execute("INSERT INTO test_nested_column_pushdown VALUES" +
                " ('evt-1', ROW('2024-06-16', 'AMERICA'))," +
                " ('evt-2', ROW('2024-06-17', 'ASIA'))," +
                " ('evt-3', ROW('2024-06-18', 'EUROPE'))");
    }

    @Test
    public void testNestedColumnPushdown()
    {
        // A projection or filter on a single field of a ROW column is rewritten by the dereference pushdown rules.
        // The connector level rule (IcebergParquetDereferencePushDown, enabled by default) renames the pushed down
        // column to the flattened subfield path while leaving the required subfield rooted at the base column, which
        // Velox rejects with "Required subfield does not match column name". These queries verify native workers
        // read nested fields correctly.
        assertQuery("SELECT statisticsinformation.processingdate FROM test_nested_column_pushdown");
        assertQuery("SELECT event_id FROM test_nested_column_pushdown WHERE statisticsinformation.processingdate = '2024-06-17'");
        assertQuery("SELECT statisticsinformation.processingdate, statisticsinformation.region FROM test_nested_column_pushdown");
        assertQuery("SELECT count(*) FROM test_nested_column_pushdown WHERE statisticsinformation.region = 'ASIA'");
    }

    @Test
    public void testNestedColumnFilterPushedToTableScan()
    {
        // Under native execution the connector level IcebergParquetDereferencePushDown rule is not registered because it
        // renames the pushed down column to the flattened subfield path ("statisticsinformation$_$_$processingdate")
        // while leaving the required subfield rooted at the base column, which Velox rejects with
        // "Required subfield does not match column name". Verify the subfield filter still reaches the table scan and
        // that every scan column keeps its base column name, the only shape Velox accepts.
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
        String query = "SELECT event_id FROM test_nested_column_pushdown WHERE statisticsinformation.processingdate = '2024-06-17'";

        assertPlan(session, query, anyTree(tableScan("test_nested_column_pushdown")), plan -> {
            // The subfield predicate is fully enforced by the scan, no residual FilterNode remains.
            assertTrue(searchFrom(plan.getRoot()).where(FilterNode.class::isInstance).findAll().isEmpty());

            TableScanNode tableScan = searchFrom(plan.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findOnlyElement();

            // No column was hoisted into a flattened "$_$_$" column by the dereference pushdown rule.
            for (ColumnHandle column : tableScan.getAssignments().values()) {
                assertFalse(isPushedDownSubfield((IcebergColumnHandle) column));
            }

            assertTrue(tableScan.getTable().getLayout().isPresent());
            IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) tableScan.getTable().getLayout().get();

            Map<Subfield, Domain> domains = layoutHandle.getDomainPredicate().getDomains().orElseThrow(AssertionError::new);
            assertEquals(domains, ImmutableMap.of(
                    new Subfield("statisticsinformation.processingdate"),
                    singleValue(VARCHAR, utf8Slice("2024-06-17"))));
            assertEquals(layoutHandle.getRemainingPredicate(), TRUE_CONSTANT);
            // The predicate column is the base ROW column, not a synthesized flattened column.
            assertEquals(layoutHandle.getPredicateColumns().keySet(), ImmutableSet.of("statisticsinformation"));
        });

        assertQuery(session, query, "VALUES ('evt-2')");
    }

    @Test
    public void testPathHiddenColumn()
    {
        assertQuery("SELECT \"$path\", * FROM test_hidden_columns");

        // Fetch one of the file paths and use it in a filter
        String filePath = (String) computeActual("SELECT \"$path\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertQuery(format("SELECT * from test_hidden_columns WHERE \"$path\"='%s'", filePath));

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$path\"='%s'", filePath))
                        .getOnlyValue(),
                1L);

        // Filter for $path that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$path\"='%s'", "non-existent-path"))
                        .getOnlyValue(),
                0L);
    }

    @Test
    public void testDataSequenceNumberHiddenColumn()
    {
        assertQuery("SELECT \"$data_sequence_number\", * FROM test_hidden_columns");

        // Fetch one of the data sequence numbers and use it in a filter
        Long dataSequenceNumber = (Long) computeActual("SELECT \"$data_sequence_number\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertQuery(format("SELECT * from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber));

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber))
                        .getOnlyValue(),
                1L);

        // Filter for $data_sequence_number that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", 1000))
                        .getOnlyValue(),
                0L);
    }

    @Test
    public void testDateQueries()
    {
        assertQuery("SELECT * FROM ice_table_partitioned WHERE ds >= date'1994-01-01'", "VALUES (1, date'2022-04-09'), (2, date'2022-03-18')");
        assertQuery("SELECT * FROM ice_table WHERE ds = date'2022-04-09'", "VALUES (1, date'2022-04-09')");
    }

    @Test
    public void testAnalyze()
    {
        assertUpdate(getSession(), "ANALYZE test_analyze", 5);
    }
}
