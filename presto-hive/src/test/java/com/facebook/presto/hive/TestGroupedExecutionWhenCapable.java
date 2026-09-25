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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION_WHEN_CAPABLE;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Verifies the {@code grouped_execution_when_capable} session property: grouped execution should engage
 * for any grouped-execution-capable (bucketed) fragment even when no downstream operator makes it
 * individually "useful". Each query below routes a bucketed scan through a shuffle on a NON-bucket key
 * (join / left join / aggregation / window), so the scan fragment is grouped-execution-capable but not
 * "useful"; with the flag off (but grouped execution on) it is not tagged grouped. A bucketed-to-bucketed
 * table write (also capable-but-not-useful) is covered too.
 */
@Test(singleThreaded = true)
public class TestGroupedExecutionWhenCapable
        extends AbstractTestQueryFramework
{
    // Inner join on v (a NON-bucket column): each input is a bucketed scan shuffled by v.
    @Language("SQL")
    private static final String JOIN_ON_NON_BUCKET_KEY =
            "SELECT count(*) FROM test_gewc a JOIN test_gewc b ON a.v = b.v";

    // Left outer join on v (NON-bucket): the probe-side bucketed scan is shuffled by v.
    @Language("SQL")
    private static final String LEFT_JOIN_ON_NON_BUCKET_KEY =
            "SELECT count(b.bkey) FROM test_gewc a LEFT JOIN test_gewc b ON a.v = b.v";

    // Aggregation grouped by v (NON-bucket): scan -> partial aggregation fragment is shuffled by v.
    @Language("SQL")
    private static final String AGG_ON_NON_BUCKET_KEY =
            "SELECT v, count(*) FROM test_gewc GROUP BY v";

    // Window partitioned by v (NON-bucket): scan fragment is shuffled by v before the window operator.
    @Language("SQL")
    private static final String WINDOW_ON_NON_BUCKET_KEY =
            "SELECT bkey, v, row_number() OVER (PARTITION BY v ORDER BY bkey) AS rn FROM test_gewc";

    // Bucketed -> bucketed copy: the scan -> table-writer fragment is capable but not "useful".
    @Language("SQL")
    private static final String BUCKETED_COPY =
            "INSERT INTO test_gewc_dst SELECT bkey, grp, v FROM test_gewc";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    private Session whenCapableEnabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(GROUPED_EXECUTION_WHEN_CAPABLE, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .build();
    }

    private Session whenCapableDisabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(GROUPED_EXECUTION_WHEN_CAPABLE, "false")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate(
                "CREATE TABLE test_gewc (\n" +
                        "  bkey BIGINT,\n" +
                        "  grp VARCHAR,\n" +
                        "  v BIGINT\n" +
                        ")\n" +
                        "WITH (bucketed_by = ARRAY['bkey'], bucket_count = 8)");
        assertUpdate(
                "INSERT INTO test_gewc " +
                        "SELECT bkey, 'g' || CAST(bkey % 10 AS VARCHAR), bkey % 25 " +
                        "FROM UNNEST(sequence(1, 200)) AS t(bkey)",
                200);
        // Identically bucketed copy target.
        assertUpdate(
                "CREATE TABLE test_gewc_dst (bkey BIGINT, grp VARCHAR, v BIGINT)\n" +
                        "WITH (bucketed_by = ARRAY['bkey'], bucket_count = 8)");
    }

    @Test
    public void testInnerJoinOnNonBucketKey()
    {
        assertGroupsOnlyWhenEnabled(JOIN_ON_NON_BUCKET_KEY);
    }

    @Test
    public void testLeftJoinOnNonBucketKey()
    {
        assertGroupsOnlyWhenEnabled(LEFT_JOIN_ON_NON_BUCKET_KEY);
    }

    @Test
    public void testAggregationOnNonBucketKey()
    {
        assertGroupsOnlyWhenEnabled(AGG_ON_NON_BUCKET_KEY);
    }

    @Test
    public void testWindowOnNonBucketKey()
    {
        assertGroupsOnlyWhenEnabled(WINDOW_ON_NON_BUCKET_KEY);
    }

    @Test
    public void testBucketedCopy()
    {
        assertTrue(
                anyStageGroupedExecution(whenCapableEnabled(), BUCKETED_COPY),
                "Expected a grouped-execution stage for the bucketed-scan -> table-writer fragment when the flag is enabled");
        // Sanity: the copy wrote every row.
        assertQuery(whenCapableEnabled(), "SELECT count(*) FROM test_gewc_dst", "SELECT 200");
    }

    /**
     * For a query whose bucketed scan is capable but not "useful": results are identical on vs off,
     * grouped execution engages when the flag is on, and does not when it is off.
     */
    private void assertGroupsOnlyWhenEnabled(@Language("SQL") String sql)
    {
        // Correctness: the flag only changes the execution strategy.
        assertQueryWithSameQueryRunner(whenCapableEnabled(), sql, whenCapableDisabled());
        assertTrue(
                anyStageGroupedExecution(whenCapableEnabled(), sql),
                "Expected a grouped-execution stage when grouped_execution_when_capable is enabled for: " + sql);
        assertFalse(
                anyStageGroupedExecution(whenCapableDisabled(), sql),
                "Did not expect grouped execution for a capable-but-not-useful fragment when the flag is disabled for: " + sql);
    }

    private boolean anyStageGroupedExecution(Session session, @Language("SQL") String sql)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(session, sql);
        QueryInfo queryInfo = queryRunner.getQueryInfo(result.getQueryId());
        assertTrue(queryInfo.getOutputStage().isPresent(), "Query should have an output stage");
        for (StageInfo stageInfo : queryInfo.getOutputStage().get().getAllStages()) {
            if (stageInfo.getPlan().isPresent() && stageInfo.getPlan().get().getStageExecutionDescriptor().isStageGroupedExecution()) {
                return true;
            }
        }
        return false;
    }
}
