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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.SystemPartitioningHandle;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_OPTIMIZER_ENABLED;
import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_PARALLELISM;
import static com.facebook.presto.SystemSessionProperties.RPC_STREAMING_MODE;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAddLocalExchanges
        extends BasePlanTest
{
    // Treat the ordinary scalar lower() as an RPC function so RpcFunctionOptimizer
    // rewrites lower(comment) into an RPCNode over a parallel TPCH table scan. This
    // lets us assert AddLocalExchanges.visitRPC's plan shape without a real RPC
    // backend. TASK_CONCURRENCY>1 keeps the scan multi-stream so the BATCH GATHER is
    // actually inserted (over a single-stream source the GATHER would be a no-op).
    private void assertRpcPlan(@Language("SQL") String sql, String streamingMode, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new RpcFunctionOptimizer(() -> ImmutableSet.of("lower")),
                new UnaliasSymbolReferences(getMetadata().getFunctionAndTypeManager()),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        getMetadata(),
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getCostCalculator(),
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new AddLocalExchanges(getMetadata(), getQueryRunner().getStatsCalculator(), false));
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .setSystemProperty(RPC_FUNCTION_OPTIMIZER_ENABLED, "true")
                .setSystemProperty(RPC_STREAMING_MODE, streamingMode)
                .build();
        assertPlan(sql, session, OPTIMIZED, pattern, optimizers);
    }

    private void assertAutomaticRemoteTaskCounts(
            @Language("SQL") String sql,
            int configuredTaskCount,
            int recommendedTaskCount,
            int expectedRpcCount,
            List<Integer> expectedTaskCounts)
    {
        RpcExecutionPolicy policy = intent -> RpcExecutionProperties.builder()
                .setStreamingMode(RPCNode.StreamingMode.BATCH)
                .setRecommendedTaskCount(recommendedTaskCount)
                .build();
        assertAutomaticRemoteTaskCounts(
                sql,
                configuredTaskCount,
                ImmutableSet.of("lower"),
                policy,
                false,
                expectedRpcCount,
                expectedTaskCounts);
    }

    private void assertAutomaticRemoteTaskCounts(
            @Language("SQL") String sql,
            int configuredTaskCount,
            Set<String> functionNames,
            RpcExecutionPolicy policy,
            boolean prestoSparkExecutionEnvironment,
            int expectedRpcCount,
            List<Integer> expectedTaskCounts)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new RpcFunctionOptimizer(() -> functionNames, null, policy),
                new UnaliasSymbolReferences(getMetadata().getFunctionAndTypeManager()),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        getMetadata(),
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getCostCalculator(),
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new AddExchanges(
                        getMetadata(),
                        new PartitioningProviderManager(),
                        false,
                        prestoSparkExecutionEnvironment));
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(RPC_FUNCTION_OPTIMIZER_ENABLED, "true")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, Integer.toString(configuredTaskCount))
                .build();
        getQueryRunner().inTransaction(session, transactionSession -> {
            Plan actualPlan = getQueryRunner().createPlan(
                    transactionSession,
                    sql,
                    optimizers,
                    OPTIMIZED,
                    WarningCollector.NOOP);
            assertEquals(
                    searchFrom(actualPlan.getRoot())
                            .where(node -> node instanceof RPCNode)
                            .count(),
                    expectedRpcCount);
            assertEquals(remoteRoundRobinTaskCounts(actualPlan), expectedTaskCounts);
            return null;
        });
    }

    private static List<Integer> remoteRoundRobinTaskCounts(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode &&
                        ((ExchangeNode) node).getScope() == REMOTE_STREAMING &&
                        ((ExchangeNode) node).getType() == REPARTITION)
                .findAll()
                .stream()
                .map(ExchangeNode.class::cast)
                .map(exchange -> exchange.getPartitioningScheme().getPartitioning().getHandle().getConnectorHandle())
                .filter(SystemPartitioningHandle.class::isInstance)
                .map(SystemPartitioningHandle.class::cast)
                .map(handle -> handle.getPartitionCount().orElseThrow(AssertionError::new))
                .collect(Collectors.toList());
    }

    @Test
    public void testBatchRpcGathersToSingleStream()
    {
        // BATCH: a LocalExchange[SINGLE/GATHER] is inserted between the RPCNode and its
        // parallel source, so the RPC stage runs single-driver while the scan below
        // stays parallel (no batch fragmentation across drivers).
        assertRpcPlan(
                "SELECT lower(comment) FROM orders",
                "BATCH",
                anyTree(
                        node(RPCNode.class,
                                exchange(LOCAL, GATHER,
                                        tableScan("orders")))));
    }

    @Test
    public void testPerRowRpcStaysParallel()
    {
        // PER_ROW: no single-stream GATHER is forced -- the RPCNode keeps the parallel
        // distribution of its source.
        assertRpcPlan(
                "SELECT lower(comment) FROM orders",
                "PER_ROW",
                anyTree(
                        node(RPCNode.class,
                                tableScan("orders"))));
    }

    @Test
    public void testAutomaticRpcParallelismIsMinimumOnly()
    {
        assertAutomaticRemoteTaskCounts(
                "SELECT lower(x) FROM (VALUES 'a') t(x)",
                0,
                4,
                1,
                ImmutableList.of(4));
        assertAutomaticRemoteTaskCounts(
                "SELECT lower(comment) FROM orders",
                0,
                4,
                1,
                ImmutableList.of());
        assertAutomaticRemoteTaskCounts(
                "SELECT lower(lower(x)) FROM (VALUES 'a') t(x)",
                0,
                4,
                2,
                ImmutableList.of(4));
    }

    @Test
    public void testAutomaticRpcParallelismPreservesKnownWiderDistribution()
    {
        RpcExecutionPolicy policy = intent -> RpcExecutionProperties.builder()
                .setStreamingMode(RPCNode.StreamingMode.BATCH)
                .setRecommendedTaskCount(intent.getFunctionName().equals("upper") ? 8 : 4)
                .build();
        assertAutomaticRemoteTaskCounts(
                "SELECT lower(upper(x)) FROM (VALUES 'a') t(x)",
                0,
                ImmutableSet.of("lower", "upper"),
                policy,
                false,
                2,
                ImmutableList.of(8));
    }

    @Test
    public void testAutomaticRpcParallelismDoesNotAddRemoteExchangeOnPrestoSpark()
    {
        RpcExecutionPolicy policy = intent -> RpcExecutionProperties.builder()
                .setStreamingMode(RPCNode.StreamingMode.BATCH)
                .setRecommendedTaskCount(4)
                .build();
        assertAutomaticRemoteTaskCounts(
                "SELECT lower(x) FROM (VALUES 'a') t(x)",
                0,
                ImmutableSet.of("lower"),
                policy,
                true,
                1,
                ImmutableList.of());
    }

    @Test
    public void testConfiguredRpcParallelismRemainsExact()
    {
        assertAutomaticRemoteTaskCounts(
                "SELECT lower(x) FROM (VALUES 'a') t(x)",
                3,
                8,
                1,
                ImmutableList.of(3));
        assertAutomaticRemoteTaskCounts(
                "SELECT lower(comment) FROM orders",
                3,
                8,
                1,
                ImmutableList.of(3));
        assertAutomaticRemoteTaskCounts(
                "SELECT lower(x) FROM (VALUES 'a') t(x)",
                1,
                8,
                1,
                ImmutableList.of());
    }

    @DataProvider
    public Object[][] rpcParentPreferences()
    {
        List<Object[]> cases = new ArrayList<>();
        for (boolean[] environment : new boolean[][] {{false, false}, {true, false}, {true, true}}) {
            for (int parallelism : new int[] {0, 8}) {
                for (RPCNode.StreamingMode mode : new RPCNode.StreamingMode[] {RPCNode.StreamingMode.BATCH, RPCNode.StreamingMode.PER_ROW}) {
                    cases.add(new Object[] {environment[0], environment[1], parallelism, mode});
                }
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "rpcParentPreferences")
    public void testGroupByRpcResultOverUnion(boolean nativeExecution, boolean sparkExecution, int parallelism, RPCNode.StreamingMode mode)
    {
        assertRpcParentPreferences(
                "SELECT lower(x), count(*) FROM (" +
                        "SELECT comment x FROM orders UNION ALL SELECT comment x FROM orders) u GROUP BY 1",
                nativeExecution, sparkExecution, parallelism, mode);
    }

    @Test(dataProvider = "rpcParentPreferences")
    public void testGroupByRpcResultWithoutUnion(boolean nativeExecution, boolean sparkExecution, int parallelism, RPCNode.StreamingMode mode)
    {
        assertRpcParentPreferences("SELECT lower(comment), count(*) FROM orders GROUP BY 1",
                nativeExecution, sparkExecution, parallelism, mode);
    }

    @Test(dataProvider = "rpcParentPreferences")
    public void testPartitionedSemiJoinOverRpcUnion(boolean nativeExecution, boolean sparkExecution, int parallelism, RPCNode.StreamingMode mode)
    {
        Plan plan = assertRpcParentPreferences(
                "SELECT orderkey FROM orders WHERE orderkey IN (" +
                        "SELECT id FROM (SELECT id, lower(x) v FROM (" +
                        "SELECT orderkey id, comment x FROM orders UNION ALL " +
                        "SELECT orderkey id, comment x FROM orders) u) r WHERE v = 'yes')",
                nativeExecution, sparkExecution, parallelism, mode);
        assertTrue(searchFrom(plan.getRoot()).where(SemiJoinNode.class::isInstance).findFirst().isPresent());
    }

    @Test(dataProvider = "rpcParentPreferences")
    public void testCountRpcResultOverUnion(boolean nativeExecution, boolean sparkExecution, int parallelism, RPCNode.StreamingMode mode)
    {
        assertRpcParentPreferences(
                "SELECT count(v) FROM (SELECT lower(x) v FROM (" +
                        "SELECT comment x FROM orders UNION ALL SELECT comment x FROM orders) u) r",
                nativeExecution, sparkExecution, parallelism, mode);
    }

    private Plan assertRpcParentPreferences(@Language("SQL") String sql, boolean nativeExecution, boolean sparkExecution, int parallelism, RPCNode.StreamingMode mode)
    {
        RpcExecutionPolicy policy = intent -> RpcExecutionProperties.builder()
                .setStreamingMode(mode)
                .setRecommendedTaskCount(1)
                .build();
        ImmutableList.Builder<PlanOptimizer> optimizers = ImmutableList.builder();
        optimizers.add(new RpcFunctionOptimizer(() -> ImmutableSet.of("lower"), null, policy));
        for (PlanOptimizer optimizer : getQueryRunner().getPlanOptimizers(false, nativeExecution)) {
            if (optimizer instanceof StatsRecordingPlanOptimizer && ((StatsRecordingPlanOptimizer) optimizer).getDelegate() instanceof AddExchanges) {
                optimizer = new AddExchanges(getMetadata(), new PartitioningProviderManager(), nativeExecution, sparkExecution);
            }
            optimizers.add(optimizer);
        }
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(RPC_FUNCTION_OPTIMIZER_ENABLED, "true")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, Integer.toString(parallelism))
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();
        Plan plan = getQueryRunner().inTransaction(session, transactionSession -> getQueryRunner().createPlan(
                transactionSession, sql, optimizers.build(), OPTIMIZED, WarningCollector.NOOP));
        assertEquals(searchFrom(plan.getRoot()).where(RPCNode.class::isInstance).count(), 1);
        RPCNode rpc = (RPCNode) searchFrom(plan.getRoot()).where(RPCNode.class::isInstance).findOnlyElement();
        assertEquals(rpc.getStreamingMode(), mode);
        return plan;
    }
}
