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
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION_FOR_PARTIAL_AGGREGATION;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Verifies the {@code grouped_execution_for_partial_aggregation} session property: grouped execution
 * should engage for a bucketed-scan fragment whose only grouped-eligible operator is a PARTIAL
 * aggregation, even though the GROUP BY keys ({@code grp}) do not match the table bucketing ({@code bkey}).
 */
@Test(singleThreaded = true)
public class TestGroupedExecutionForPartialAggregation
        extends AbstractTestQueryFramework
{
    // Bucketed by bkey; GROUP BY is on grp (a non-bucket column), so the partial aggregation is on non-bucket keys.
    @Language("SQL")
    private static final String PARTIAL_AGG_ON_NON_BUCKET_KEY = "SELECT grp, count(*) FROM test_gepa GROUP BY grp";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    private Session groupedPartialAggEnabledSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(GROUPED_EXECUTION_FOR_PARTIAL_AGGREGATION, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .build();
    }

    private Session groupedPartialAggDisabledSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(GROUPED_EXECUTION_FOR_PARTIAL_AGGREGATION, "false")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate(
                "CREATE TABLE test_gepa (\n" +
                        "  bkey BIGINT,\n" +
                        "  grp VARCHAR,\n" +
                        "  v DOUBLE\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  bucketed_by = ARRAY['bkey'],\n" +
                        "  bucket_count = 8\n" +
                        ")");
        // 200 rows, 10 distinct groups (grp) spread across all buckets so each group spans multiple buckets.
        assertUpdate(
                "INSERT INTO test_gepa " +
                        "SELECT bkey, 'g' || CAST(bkey % 10 AS VARCHAR), CAST(bkey AS DOUBLE) " +
                        "FROM UNNEST(sequence(1, 200)) AS t(bkey)",
                200);
    }

    @Test
    public void testResultsUnchangedWithGroupedPartialAggregation()
    {
        // Correctness: the flag only changes the execution strategy, so results must be identical on vs off.
        assertQueryWithSameQueryRunner(groupedPartialAggEnabledSession(), PARTIAL_AGG_ON_NON_BUCKET_KEY, groupedPartialAggDisabledSession());
    }

    @Test
    public void testGroupedExecutionEngagesWhenEnabled()
    {
        assertTrue(
                anyStageGroupedExecution(groupedPartialAggEnabledSession(), PARTIAL_AGG_ON_NON_BUCKET_KEY),
                "Expected a grouped-execution stage for the bucketed-scan -> partial-aggregation fragment when the flag is enabled");
    }

    @Test
    public void testGroupedExecutionDoesNotEngageWhenDisabled()
    {
        // With the flag off, a partial aggregation on non-bucket keys is not "useful" enough to tag the fragment grouped.
        assertFalse(
                anyStageGroupedExecution(groupedPartialAggDisabledSession(), PARTIAL_AGG_ON_NON_BUCKET_KEY),
                "Did not expect grouped execution for the partial-aggregation fragment when the flag is disabled");
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
