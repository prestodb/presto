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
package com.facebook.presto.nativetests;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.sessionpropertyproviders.NativeWorkerSessionPropertyProvider.NATIVE_HASH_PROBE_FINISH_EARLY_ON_EMPTY_BUILD;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * End-to-end coverage of {@code native_hash_probe_finish_early_on_empty_build}. The flag applies
 * only to join types for which Velox's {@code HashProbe::skipProbeOnEmptyBuild()} returns true:
 * inner, left semi filter, right, right semi filter, and right semi project. For all other join
 * types (left outer, full outer, anti, left semi project) the flag is a no-op — the probe side
 * must still emit rows even when the build is empty.
 * <p>
 * Tests run against a real external native worker (via {@link
 * PrestoNativeQueryRunnerUtils#nativeHiveQueryRunnerBuilder()}) with {@code
 * join_distribution_type=PARTITIONED} to force a probe-side shuffle. This puts an
 * {@code ExchangeClient} between the upstream probe scan tasks and the hash-probe operator, which
 * is what the flag closes early when the build side is empty.
 * <p>
 * For covered join types, tests assert both:
 * <ul>
 *   <li>Correctness — result matches the Java baseline with the flag on and with the flag off.</li>
 *   <li>Actual cancellation — the {@code LookupJoinOperator} on the probe side processes strictly
 *       fewer rows with the flag on than with the flag off. With the flag off, Velox drains the
 *       full probe stream via {@code skipInput_} without a downstream abort, so upstream sends
 *       every row. With the flag on, {@code HashProbe} calls {@code noMoreInput()} immediately,
 *       closing the probe-side {@code ExchangeClient} and aborting the upstream producer tasks
 *       before they emit the rest of the scan.</li>
 * </ul>
 * <p>
 * For no-op join types, only correctness is asserted; the cancellation path is never taken.
 * <p>
 * The subquery predicate {@code orderkey > 99999999999} keeps the build side visibly non-empty at
 * plan time so the join is not eliminated by static optimizers but produces zero rows at runtime.
 */
public class TestNativeHashProbeFinishEarlyOnEmptyBuild
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    private Session partitionedJoinWithFlag(boolean value)
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(NATIVE_HASH_PROBE_FINISH_EARLY_ON_EMPTY_BUILD, String.valueOf(value))
                .build();
    }

    /**
     * For join types that trigger the early-finish path. Verifies both correctness (results
     * match the Java baseline in both flag states) and that the probe-side {@code LookupJoin}
     * operator receives strictly fewer rows with the flag on — evidence that the upstream probe
     * source tasks were aborted rather than drained.
     */
    private void assertProbeCancelledOnEmptyBuild(@Language("SQL") String sql)
    {
        Session off = partitionedJoinWithFlag(false);
        Session on = partitionedJoinWithFlag(true);

        assertQuery(off, sql);
        assertQuery(on, sql);

        DistributedQueryRunner runner = getDistributedQueryRunner();
        QueryId qOff = runner.executeWithQueryId(off, sql).getQueryId();
        QueryId qOn = runner.executeWithQueryId(on, sql).getQueryId();
        long probeRowsOff = lookupJoinInputPositions(runner.getQueryInfo(qOff));
        long probeRowsOn = lookupJoinInputPositions(runner.getQueryInfo(qOn));

        assertTrue(
                probeRowsOff > 0,
                format("expected the flag-off run to drain probe rows, got %d", probeRowsOff));
        assertTrue(
                probeRowsOn < probeRowsOff,
                format(
                        "expected fewer probe rows with the flag on (upstream aborted), " +
                                "but got on=%d, off=%d",
                        probeRowsOn,
                        probeRowsOff));
    }

    /**
     * For cases where the flag must be a no-op — either a join type outside {@code
     * skipProbeOnEmptyBuild()}, or a non-empty build side on any join type. Verifies correctness
     * in both flag states and that the probe-side {@code LookupJoin} operator's input-row count
     * is unchanged, so the flag never truncates probe input outside its intended path.
     */
    private void assertFlagIsNoOp(@Language("SQL") String sql)
    {
        Session off = partitionedJoinWithFlag(false);
        Session on = partitionedJoinWithFlag(true);

        assertQuery(off, sql);
        assertQuery(on, sql);

        DistributedQueryRunner runner = getDistributedQueryRunner();
        QueryId qOff = runner.executeWithQueryId(off, sql).getQueryId();
        QueryId qOn = runner.executeWithQueryId(on, sql).getQueryId();
        long probeRowsOff = lookupJoinInputPositions(runner.getQueryInfo(qOff));
        long probeRowsOn = lookupJoinInputPositions(runner.getQueryInfo(qOn));

        assertEquals(
                probeRowsOn,
                probeRowsOff,
                format(
                        "flag must be a no-op for this join type but probe input differs: " +
                                "on=%d, off=%d",
                        probeRowsOn,
                        probeRowsOff));
    }

    private static long lookupJoinInputPositions(QueryInfo queryInfo)
    {
        StageInfo output = queryInfo.getOutputStage()
                .orElseThrow(() -> new AssertionError("query has no output stage"));
        return output.getAllStages().stream()
                .flatMap(stage -> stage.getLatestAttemptExecutionInfo().getTasks().stream())
                .flatMap(task -> task.getStats().getPipelines().stream())
                .flatMap(pipeline -> pipeline.getOperatorSummaries().stream())
                .filter(op -> op.getOperatorType().equals("LookupJoinOperator"))
                .mapToLong(OperatorStats::getRawInputPositions)
                .sum();
    }

    // Join types that trigger the early-finish path (skipProbeOnEmptyBuild() == true).

    @Test
    public void testInnerJoinWithEmptyBuild()
    {
        assertProbeCancelledOnEmptyBuild(
                "SELECT o.orderkey, o.custkey " +
                        "FROM orders o " +
                        "INNER JOIN (SELECT orderkey FROM orders WHERE orderkey > 99999999999) e " +
                        "  ON o.orderkey = e.orderkey");
    }

    @Test
    public void testLeftSemiFilterWithEmptyBuild()
    {
        assertProbeCancelledOnEmptyBuild(
                "SELECT o.orderkey " +
                        "FROM orders o " +
                        "WHERE o.orderkey IN (SELECT orderkey FROM orders WHERE orderkey > 99999999999)");
    }

    @Test
    public void testRightJoinWithEmptyBuild()
    {
        assertProbeCancelledOnEmptyBuild(
                "SELECT o.orderkey, e.k " +
                        "FROM orders o " +
                        "RIGHT JOIN (SELECT orderkey AS k FROM orders WHERE orderkey > 99999999999) e " +
                        "  ON o.orderkey = e.k");
    }

    // Same covered join types with a non-empty build side — the empty-build path is never taken,
    // so the flag must not change the result or the number of probe rows processed.

    @Test
    public void testInnerJoinWithNonEmptyBuild()
    {
        assertFlagIsNoOp(
                "SELECT o.orderkey, o.custkey " +
                        "FROM orders o " +
                        "INNER JOIN (SELECT orderkey FROM orders WHERE orderkey < 100) e " +
                        "  ON o.orderkey = e.orderkey");
    }

    @Test
    public void testLeftSemiFilterWithNonEmptyBuild()
    {
        assertFlagIsNoOp(
                "SELECT o.orderkey " +
                        "FROM orders o " +
                        "WHERE o.orderkey IN (SELECT orderkey FROM orders WHERE orderkey < 100)");
    }

    @Test
    public void testRightJoinWithNonEmptyBuild()
    {
        assertFlagIsNoOp(
                "SELECT o.orderkey, e.k " +
                        "FROM orders o " +
                        "RIGHT JOIN (SELECT orderkey AS k FROM orders WHERE orderkey < 100) e " +
                        "  ON o.orderkey = e.k");
    }

    // Join types where the flag has no effect and probe rows must still be emitted.

    @Test
    public void testLeftJoinWithEmptyBuild()
    {
        assertFlagIsNoOp(
                "SELECT o.orderkey, e.k " +
                        "FROM orders o " +
                        "LEFT JOIN (SELECT orderkey AS k FROM orders WHERE orderkey > 99999999999) e " +
                        "  ON o.orderkey = e.k");
    }

    @Test
    public void testFullJoinWithEmptyBuild()
    {
        assertFlagIsNoOp(
                "SELECT o.orderkey, e.k " +
                        "FROM orders o " +
                        "FULL OUTER JOIN (SELECT orderkey AS k FROM orders WHERE orderkey > 99999999999) e " +
                        "  ON o.orderkey = e.k");
    }

    @Test
    public void testAntiJoinWithEmptyBuild()
    {
        assertFlagIsNoOp(
                "SELECT o.orderkey " +
                        "FROM orders o " +
                        "WHERE o.orderkey NOT IN (SELECT orderkey FROM orders WHERE orderkey > 99999999999)");
    }
}
