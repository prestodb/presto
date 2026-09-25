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
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_OPTIMIZER_ENABLED;
import static com.facebook.presto.SystemSessionProperties.RPC_STREAMING_MODE;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;

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
}
