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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.PARTITION_AWARE_GROUPED_EXECUTION;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPartitionAwareGroupedExecution
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    private Session partitionAwareSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(PARTITION_AWARE_GROUPED_EXECUTION, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .build();
    }

    private Session partitionAwareBroadcastSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(PARTITION_AWARE_GROUPED_EXECUTION, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    private Session partitionAwarePartitionedSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(PARTITION_AWARE_GROUPED_EXECUTION, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .build();
    }

    private Session standardGroupedSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(PARTITION_AWARE_GROUPED_EXECUTION, "false")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .build();
    }

    /**
     * Verify that partition-aware grouped execution is actually used (not fallen back to standard).
     * Checks that at least one stage has non-empty groupedExecutionPartitionValues.
     */
    private void assertPartitionAwareUsed(Session session, @Language("SQL") String sql)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(session, sql);
        QueryInfo queryInfo = queryRunner.getQueryInfo(result.getQueryId());
        assertTrue(queryInfo.getOutputStage().isPresent(), "Query should have an output stage");

        boolean foundPartitionAware = false;
        for (StageInfo stageInfo : queryInfo.getOutputStage().get().getAllStages()) {
            if (stageInfo.getPlan().isPresent()) {
                List<Map<String, String>> partitionValues =
                        stageInfo.getPlan().get().getStageExecutionDescriptor().getGroupedExecutionPartitionValues();
                if (!partitionValues.isEmpty()) {
                    foundPartitionAware = true;
                    break;
                }
            }
        }
        assertTrue(foundPartitionAware, "Expected partition-aware grouped execution but no stage has partition values. Query: " + sql);
    }

    /**
     * Verify that partition-aware grouped execution is NOT used (standard grouped execution).
     */
    private void assertPartitionAwareNotUsed(Session session, @Language("SQL") String sql)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(session, sql);
        QueryInfo queryInfo = queryRunner.getQueryInfo(result.getQueryId());
        assertTrue(queryInfo.getOutputStage().isPresent(), "Query should have an output stage");

        for (StageInfo stageInfo : queryInfo.getOutputStage().get().getAllStages()) {
            if (stageInfo.getPlan().isPresent()) {
                List<Map<String, String>> partitionValues =
                        stageInfo.getPlan().get().getStageExecutionDescriptor().getGroupedExecutionPartitionValues();
                assertTrue(partitionValues.isEmpty(),
                        "Expected standard grouped execution but found partition values: " + partitionValues);
            }
        }
    }

    @BeforeClass
    public void setUp()
    {
        // t1: bucketed by col into 8 buckets, partitioned by ds, 3 ds values, 100 rows per ds
        assertUpdate(
                "CREATE TABLE test_pa_t1 (\n" +
                        "  col BIGINT,\n" +
                        "  val1 VARCHAR,\n" +
                        "  val2 DOUBLE,\n" +
                        "  ds VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  partitioned_by = ARRAY['ds'],\n" +
                        "  bucketed_by = ARRAY['col'],\n" +
                        "  bucket_count = 8\n" +
                        ")");

        // t2: same structure as t1
        assertUpdate(
                "CREATE TABLE test_pa_t2 (\n" +
                        "  col BIGINT,\n" +
                        "  val3 VARCHAR,\n" +
                        "  val4 DOUBLE,\n" +
                        "  ds VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  partitioned_by = ARRAY['ds'],\n" +
                        "  bucketed_by = ARRAY['col'],\n" +
                        "  bucket_count = 8\n" +
                        ")");

        // t3: multi-partition-key table (ds, hr), only ds=01,02 (no ds=03), cols 1-50
        assertUpdate(
                "CREATE TABLE test_pa_t3 (\n" +
                        "  col BIGINT,\n" +
                        "  val5 VARCHAR,\n" +
                        "  ds VARCHAR,\n" +
                        "  hr VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  partitioned_by = ARRAY['ds', 'hr'],\n" +
                        "  bucketed_by = ARRAY['col'],\n" +
                        "  bucket_count = 8\n" +
                        ")");

        // Populate t1: 100 rows per ds, 3 ds values = 300 total rows
        assertUpdate(
                "INSERT INTO test_pa_t1 " +
                        "SELECT col, 'val1_' || CAST(col AS VARCHAR), CAST(col AS DOUBLE) * 1.5, ds " +
                        "FROM UNNEST(sequence(1, 100)) AS t(col) " +
                        "CROSS JOIN (VALUES '2024-01-01', '2024-01-02', '2024-01-03') AS dates(ds)",
                300);

        // Populate t2: same distribution as t1
        assertUpdate(
                "INSERT INTO test_pa_t2 " +
                        "SELECT col, 'val3_' || CAST(col AS VARCHAR), CAST(col AS DOUBLE) * 2.5, ds " +
                        "FROM UNNEST(sequence(1, 100)) AS t(col) " +
                        "CROSS JOIN (VALUES '2024-01-01', '2024-01-02', '2024-01-03') AS dates(ds)",
                300);

        // Populate t3: cols 1-50, ds=01,02, hr=00,12 => 50 * 2 * 2 = 200 rows
        assertUpdate(
                "INSERT INTO test_pa_t3 " +
                        "SELECT col, 'val5_' || CAST(col AS VARCHAR), ds, hr " +
                        "FROM UNNEST(sequence(1, 50)) AS t(col) " +
                        "CROSS JOIN (VALUES '2024-01-01', '2024-01-02') AS dates(ds) " +
                        "CROSS JOIN (VALUES '00', '12') AS hours(hr)",
                200);

        // t4: same data as t1 but partition column named "ts" instead of "ds"
        assertUpdate(
                "CREATE TABLE test_pa_t4 (\n" +
                        "  col BIGINT,\n" +
                        "  val6 VARCHAR,\n" +
                        "  ts VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  partitioned_by = ARRAY['ts'],\n" +
                        "  bucketed_by = ARRAY['col'],\n" +
                        "  bucket_count = 8\n" +
                        ")");

        // Populate t4: same as t1 but using "ts" column
        assertUpdate(
                "INSERT INTO test_pa_t4 " +
                        "SELECT col, 'val6_' || CAST(col AS VARCHAR), ts " +
                        "FROM UNNEST(sequence(1, 100)) AS t(col) " +
                        "CROSS JOIN (VALUES '2024-01-01', '2024-01-02', '2024-01-03') AS dates(ts)",
                300);

        // t5: DATE partition column (not VARCHAR) — tests native type conversion
        assertUpdate(
                "CREATE TABLE test_pa_t5 (\n" +
                        "  col BIGINT,\n" +
                        "  val7 VARCHAR,\n" +
                        "  dt DATE\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  partitioned_by = ARRAY['dt'],\n" +
                        "  bucketed_by = ARRAY['col'],\n" +
                        "  bucket_count = 8\n" +
                        ")");

        // Populate t5: 100 rows per dt, 3 date values
        assertUpdate(
                "INSERT INTO test_pa_t5 " +
                        "SELECT col, 'val7_' || CAST(col AS VARCHAR), dt " +
                        "FROM UNNEST(sequence(1, 100)) AS t(col) " +
                        "CROSS JOIN (VALUES DATE '2024-01-01', DATE '2024-01-02', DATE '2024-01-03') AS dates(dt)",
                300);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS test_pa_t1");
        assertUpdate("DROP TABLE IF EXISTS test_pa_t2");
        assertUpdate("DROP TABLE IF EXISTS test_pa_t3");
        assertUpdate("DROP TABLE IF EXISTS test_pa_t4");
        assertUpdate("DROP TABLE IF EXISTS test_pa_t5");
    }

    @Test
    public void testSimpleBucketJoinOnColAndDs()
    {
        // Join on both bucket col and partition ds
        // Expected: 300 (100 cols * 3 ds)
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertQuery(partitionAwareSession(), query, "SELECT 300");
        assertQuery(standardGroupedSession(), query, "SELECT 300");
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testJoinWithGroupBy()
    {
        // Group by ds after join on col and ds
        @Language("SQL") String query =
                "SELECT t1.ds, count(*), sum(t1.val2), sum(t2.val4) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testAggregationPerColAndDs()
    {
        // Fine-grained aggregation per (col, ds)
        @Language("SQL") String query =
                "SELECT t1.col, t1.ds, count(*), sum(t1.val2 + t2.val4) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "GROUP BY t1.col, t1.ds ORDER BY t1.col, t1.ds LIMIT 6";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testPartialPartitionKey()
    {
        // t3 partitioned by (ds, hr), join only on ds — uses partition-aware with partial key (only ds)
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testPartialPartitionKeyWithGroupBy()
    {
        @Language("SQL") String query =
                "SELECT t1.ds, count(*), count(t3.val5) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testLeftJoinMismatchedPartitions()
    {
        // t1 has ds=01,02,03; t3 has ds=01,02 only
        // ds=03 rows from t1 should appear with NULLs for t3 columns
        @Language("SQL") String query =
                "SELECT t1.ds, count(*), count(t3.val5) " +
                        "FROM test_pa_t1 t1 " +
                        "LEFT JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testRightJoin()
    {
        @Language("SQL") String query =
                "SELECT t3.ds, t3.hr, count(*), count(t1.val1) " +
                        "FROM test_pa_t1 t1 " +
                        "RIGHT JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds " +
                        "GROUP BY t3.ds, t3.hr ORDER BY t3.ds, t3.hr";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testJoinOnBucketColOnly()
    {
        // Join only on bucket col (no partition col in join) — falls back to standard grouped execution
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareNotUsed(partitionAwareSession(), query);
    }

    @Test
    public void testSelfJoin()
    {
        // Self-join on col and ds
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t1 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertQuery(partitionAwareSession(), query, "SELECT 300");
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testThreeWayJoin()
    {
        // Three-way join on col and ds
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testWindowFunction()
    {
        // Window function over join result
        @Language("SQL") String query =
                "SELECT ds, col, val2, rn FROM (" +
                        "  SELECT t1.ds, t1.col, t1.val2, " +
                        "    row_number() OVER (PARTITION BY t1.ds ORDER BY t1.val2 DESC) AS rn " +
                        "  FROM test_pa_t1 t1 " +
                        "  JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds" +
                        ") WHERE rn <= 3 ORDER BY ds, rn";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testFullOuterJoin()
    {
        @Language("SQL") String query =
                "SELECT COALESCE(t1.ds, t3.ds) AS ds, count(*), count(t1.val1), count(t3.val5) " +
                        "FROM test_pa_t1 t1 " +
                        "FULL OUTER JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds " +
                        "GROUP BY COALESCE(t1.ds, t3.ds) ORDER BY 1";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        // FULL OUTER JOIN — partition-aware is not used for this query plan
        // (optimizer may choose a plan where partition columns don't survive through FULL OUTER JOIN)
    }

    @Test
    public void testJoinWithWhereFilter()
    {
        // WHERE t1.ds = '2024-01-02' filters both sides to a single partition.
        // The optimizer removes t1.ds = t2.ds from the join condition (constant folding),
        // so ds is no longer an equi-join key → partition-aware not activated.
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "WHERE t1.ds = '2024-01-02'";

        assertQuery(partitionAwareSession(), query, "SELECT 100");
    }

    @Test
    public void testJoinWithMultipleWhereFilters()
    {
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "WHERE t1.ds IN ('2024-01-01', '2024-01-03') AND t1.col > 50";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testGroupByColOnly()
    {
        // GROUP BY col only (without ds) — the FINAL aggregation filters out ds from usable
        // partition columns since ds is not in GROUP BY keys. Falls back to standard.
        @Language("SQL") String query =
                "SELECT t1.col, count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "GROUP BY t1.col ORDER BY t1.col LIMIT 5";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareNotUsed(partitionAwareSession(), query);
    }

    @Test
    public void testSubqueryWithJoin()
    {
        @Language("SQL") String query =
                "SELECT count(*) FROM (" +
                        "  SELECT t1.col, t1.ds, t1.val2 FROM test_pa_t1 t1 " +
                        "  JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "  WHERE t1.val2 > 100" +
                        ")";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testCountDistinct()
    {
        @Language("SQL") String query =
                "SELECT count(DISTINCT t1.ds) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertQuery(partitionAwareSession(), query, "SELECT 3");
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testMultipleAggregations()
    {
        @Language("SQL") String query =
                "SELECT t1.ds, min(t1.col), max(t1.col), avg(t1.val2), count(DISTINCT t1.col) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testLeftJoinSamePartitionStructure()
    {
        // t1 LEFT JOIN t2, both have same partitions — all rows should match
        @Language("SQL") String query =
                "SELECT t1.ds, count(*), count(t2.val3) " +
                        "FROM test_pa_t1 t1 " +
                        "LEFT JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testAntiJoinPattern()
    {
        // LEFT JOIN WHERE right IS NULL: find rows in t1 with no match in t3
        // t3 has cols 1-50, so cols 51-100 from t1 have no match for ds=01,02
        // ds=03 has no t3 data at all, so all 100 rows unmatched
        @Language("SQL") String query =
                "SELECT t1.ds, count(*) FROM test_pa_t1 t1 " +
                        "LEFT JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds " +
                        "WHERE t3.col IS NULL " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testUnionAllOfJoins()
    {
        @Language("SQL") String query =
                "SELECT ds, total FROM (" +
                        "  SELECT t1.ds, count(*) AS total FROM test_pa_t1 t1 " +
                        "  JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "  GROUP BY t1.ds " +
                        "  UNION ALL " +
                        "  SELECT t1.ds, count(*) AS total FROM test_pa_t1 t1 " +
                        "  JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "  GROUP BY t1.ds" +
                        ") ORDER BY ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testOrderByWithLimit()
    {
        @Language("SQL") String query =
                "SELECT t1.col, t1.ds, t1.val2 FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "ORDER BY t1.val2 DESC, t1.ds, t1.col LIMIT 5";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testPartitionAwareMatchesStandardResults()
    {
        // Verify exact result match between partition-aware ON and OFF for a complex query
        @Language("SQL") String query =
                "SELECT t1.ds, sum(t1.val2 * t2.val4) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testBroadcastJoin()
    {
        // Force broadcast join — uses DynamicLifespanScheduler with DynamicBucketNodeMap
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertQuery(partitionAwareBroadcastSession(), query, "SELECT 300");
        assertQueryWithSameQueryRunner(partitionAwareBroadcastSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareBroadcastSession(), query);
    }

    @Test
    public void testPartitionedJoin()
    {
        // Force partitioned join — uses FixedLifespanScheduler with FixedBucketNodeMap
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertQuery(partitionAwarePartitionedSession(), query, "SELECT 300");
        assertQueryWithSameQueryRunner(partitionAwarePartitionedSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwarePartitionedSession(), query);
    }

    @Test
    public void testBroadcastJoinWithGroupBy()
    {
        @Language("SQL") String query =
                "SELECT t1.ds, count(*), sum(t1.val2) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareBroadcastSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareBroadcastSession(), query);
    }

    @Test
    public void testBroadcastLeftJoinMismatchedPartitions()
    {
        // Broadcast LEFT JOIN with mismatched partitions — tests DynamicBucketNodeMap path
        @Language("SQL") String query =
                "SELECT t1.ds, count(*), count(t3.val5) " +
                        "FROM test_pa_t1 t1 " +
                        "LEFT JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareBroadcastSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareBroadcastSession(), query);
    }

    @Test
    public void testVerifyPartitionAwareUsedForFullPartitionKey()
    {
        // Verify partition-aware execution is actually used (not fallen back) for full partition key join
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testVerifyPartitionAwareUsedForPartialPartitionKey()
    {
        // Verify partition-aware is used for partial partition key (ds only, not hr)
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t3 t3 ON t1.col = t3.col AND t1.ds = t3.ds";

        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testVerifyPartitionAwareNotUsedWhenDisabled()
    {
        // Verify partition-aware is NOT used when the session property is off
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertPartitionAwareNotUsed(standardGroupedSession(), query);
    }

    @Test
    public void testVerifyPartitionAwareNotUsedForBucketOnlyJoin()
    {
        // Verify partition-aware is NOT used when no partition column is in the join
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col";

        assertPartitionAwareNotUsed(partitionAwareSession(), query);
    }

    @Test
    public void testDifferentPartitionColumnNames()
    {
        // t1 partitioned by "ds", t4 partitioned by "ts" — different column names for same concept.
        // Join on t1.ds = t4.ts. Canonical name resolution maps ts→ds (alphabetically first).
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t4 t4 ON t1.col = t4.col AND t1.ds = t4.ts";

        assertQuery(partitionAwareSession(), query, "SELECT 300");
        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testDifferentPartitionColumnNamesWithGroupBy()
    {
        // Different column names, with GROUP BY on the canonical column
        @Language("SQL") String query =
                "SELECT t1.ds, count(*), sum(t1.val2) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t4 t4 ON t1.col = t4.col AND t1.ds = t4.ts " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testDifferentPartitionColumnNamesLeftJoin()
    {
        // LEFT JOIN with different column names — t4 has ts=01,02,03, same values as t1.ds
        @Language("SQL") String query =
                "SELECT t1.ds, count(*), count(t4.val6) " +
                        "FROM test_pa_t1 t1 " +
                        "LEFT JOIN test_pa_t4 t4 ON t1.col = t4.col AND t1.ds = t4.ts " +
                        "GROUP BY t1.ds ORDER BY t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testPartitionColumnNotInOutput()
    {
        // ds is used in the join but NOT in the SELECT or GROUP BY output.
        // Partition-aware is still used because ds participates in a joined
        // equivalence class (t1.ds = t2.ds) and the visitProject preservation
        // keeps it alive through projections above the join.
        @Language("SQL") String query =
                "SELECT sum(t1.val2 + t2.val4) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testPartitionColumnNotInGroupBy()
    {
        // ds is in the join but NOT in GROUP BY.
        // The FINAL aggregation filters out ds from usable partition columns,
        // so partition-aware falls back to standard grouped execution.
        @Language("SQL") String query =
                "SELECT t1.col, sum(t1.val2) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "GROUP BY t1.col ORDER BY t1.col LIMIT 5";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareNotUsed(partitionAwareSession(), query);
    }

    @Test
    public void testPartitionColumnNotInOutputNoGroupBy()
    {
        // Global aggregation with no GROUP BY — ds is consumed by the join only.
        // Partition-aware IS used because the aggregation is split into PARTIAL (within lifespan)
        // and FINAL (above exchange). The PARTIAL step passes through ds, so it remains usable.
        @Language("SQL") String query =
                "SELECT count(*), min(t1.val2), max(t2.val4) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareUsed(partitionAwareSession(), query);
    }

    @Test
    public void testPartitionColumnMismatchNotUsed()
    {
        // t1 partitioned by ds, t2 partitioned by ds.
        // Join on col only (NOT ds) — partition columns don't match as join keys.
        // Partition-aware should NOT be used.
        @Language("SQL") String query =
                "SELECT t1.ds, t2.ds, count(*) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col " +
                        "GROUP BY t1.ds, t2.ds ORDER BY t1.ds, t2.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
        assertPartitionAwareNotUsed(partitionAwareSession(), query);
    }

    @Test
    public void testPartitionColumnFilteredToSingleValue()
    {
        // Both sides filtered to single partition value.
        // Partition-aware not activated (only 1 partition value, no benefit).
        @Language("SQL") String query =
                "SELECT count(*) " +
                        "FROM test_pa_t1 t1 " +
                        "JOIN test_pa_t2 t2 ON t1.col = t2.col AND t1.ds = t2.ds " +
                        "WHERE t1.ds = '2024-01-01' AND t2.ds = '2024-01-01'";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
    }

    @Test
    public void testSelfJoinOnSameTable()
    {
        // Self-join: same table scanned twice with same ColumnHandle for both sides.
        // Tests that union-find handles identical TableScanColumn from same table correctly.
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t1 a " +
                        "JOIN test_pa_t1 b ON a.col = b.col AND a.ds = b.ds " +
                        "WHERE a.val1 <> b.val1";

        // val1 is deterministic (val1_N), so a.val1 <> b.val1 is always false for matching col
        assertQuery(partitionAwareSession(), query, "SELECT 0");
    }

    @Test
    public void testDatePartitionColumn()
    {
        // DATE partition column — tests that epoch-day native values are converted
        // to ISO date strings to match HivePartitionKey format.
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t5 a " +
                        "JOIN test_pa_t5 b ON a.col = b.col AND a.dt = b.dt";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
    }

    @Test
    public void testDatePartitionColumnJoinWithVarcharPartition()
    {
        // Cross-type: DATE partition joined with VARCHAR partition via CAST.
        // partition-aware should not activate here (different types, cast breaks equi-join optimization).
        // Verify query still produces correct results.
        @Language("SQL") String query =
                "SELECT count(*) FROM test_pa_t5 t5 " +
                        "JOIN test_pa_t1 t1 ON t5.col = t1.col AND CAST(t5.dt AS VARCHAR) = t1.ds";

        assertQueryWithSameQueryRunner(partitionAwareSession(), query, standardGroupedSession());
    }
}
