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
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PartitioningUtils.isPartitionedOn;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Regression tests for wrong results with Bernoulli + ANTI JOIN / NOT EXISTS.
 *
 * Root cause is in Presto planner, not Velox: isPartitionedOn(SINGLE, [joinKeys]) returned true
 * for SINGLE distribution with empty args, causing AddExchanges to think a LIMIT subquery (SINGLE)
 * is already hash-partitioned on join keys and skip adding the required HASH exchange on probe side
 * for PARTITIONED anti-join. Without co-partitioning, probe rows don't land in build's matching
 * hash partition, miss their match, and anti-join incorrectly keeps them, returning non-zero
 * instead of 0. There is no cloning involved – the probe is not cloned, it's just not
 * co-partitioned.
 *
 * Native execution previously used defaultParallelism for build side when
 * native_enforce_join_build_input_partition was false, so it did not add
 * LocalExchange[HASH] on probe for grouped execution, while Java always enforced
 * hash and worked. After fix, both enforce hash and add required exchange.
 */
public class TestBernoulliAntiJoin
        extends BasePlanTest
{
    private PlanNode getOptimizedPlan(String sql, Session session)
    {
        return getQueryRunner().inTransaction(session, transactionSession ->
                getQueryRunner().createPlan(
                        transactionSession,
                        sql,
                        getQueryRunner().getPlanOptimizers(true),
                        com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED,
                        com.facebook.presto.spi.WarningCollector.NOOP).getRoot());
    }

    private static boolean containsNode(PlanNode node, Class<?> nodeClass)
    {
        if (nodeClass.isInstance(node)) {
            return true;
        }
        return node.getSources().stream().anyMatch(source -> containsNode(source, nodeClass));
    }

    @SuppressWarnings("unchecked")
    private static <T extends PlanNode> T findFirst(PlanNode node, Class<T> nodeClass)
    {
        if (nodeClass.isInstance(node)) {
            return (T) node;
        }
        for (PlanNode source : node.getSources()) {
            T found = findFirst(source, nodeClass);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static boolean containsHashExchange(PlanNode node)
    {
        if (node instanceof ExchangeNode) {
            ExchangeNode exchange = (ExchangeNode) node;
            if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                return exchange.getPartitioningScheme().getPartitioning().getArguments().size() > 0;
            }
            if (exchange.getScope() == ExchangeNode.Scope.LOCAL) {
                // For grouped execution (bucketed tables), local HASH exchange is used for co-partitioning
                return exchange.getPartitioningScheme().getPartitioning().getHandle().toString().contains("HASH") ||
                        exchange.getPartitioningScheme().getPartitioning().getArguments().size() > 0;
            }
        }
        return node.getSources().stream().anyMatch(TestBernoulliAntiJoin::containsHashExchange);
    }

    @Test
    public void testIsPartitionedOnSingleWithEmptyArgsAndEmptyColumnsIsTrue()
    {
        Partitioning single = Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of());
        assertTrue(isPartitionedOn(single, ImmutableList.of(), ImmutableSet.of()),
                "SINGLE with empty args should be considered partitioned on empty columns");
    }

    @Test
    public void testIsPartitionedOnSingleWithEmptyArgsAndNonEmptyColumnsIsFalse()
    {
        // Core fix: Before fix, this returned true, causing AddExchanges to skip HASH exchange on probe
        // side that is SINGLE (e.g., LIMIT subquery). For PARTITIONED join requiring HASH, SINGLE is NOT
        // hash-partitioned on join keys, so must return false to force repartitioning for correctness.
        VariableReferenceExpression col = new VariableReferenceExpression(Optional.empty(), "col", BIGINT);
        Partitioning single = Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of());
        assertFalse(isPartitionedOn(single, ImmutableList.of(col), ImmutableSet.of()),
                "SINGLE with empty args should NOT be considered partitioned on non-empty join keys; " +
                        "otherwise probe side SINGLE would not be hash-repartitioned to match build side, " +
                        "causing anti-join to miss matches");

        Partitioning coordinator = Partitioning.create(COORDINATOR_DISTRIBUTION, ImmutableList.of());
        assertFalse(isPartitionedOn(coordinator, ImmutableList.of(col), ImmutableSet.of()),
                "COORDINATOR with empty args should also not be considered partitioned on non-empty keys");
    }

    @Test
    public void testIsPartitionedOnHashWithEmptyArgsIsFalse()
    {
        Partitioning hashEmpty = Partitioning.create(
                com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION,
                ImmutableList.of());
        VariableReferenceExpression col = new VariableReferenceExpression(Optional.empty(), "col", BIGINT);
        assertFalse(isPartitionedOn(hashEmpty, ImmutableList.of(col), ImmutableSet.of()),
                "HASH with empty args should not be considered partitioned on specific columns");
    }

    @Test
    public void testAntiJoinWithBernoulliMustHaveCoPartitioning()
    {
        // Unit test that verifies plan has proper co-partitioning for anti-join with Bernoulli.
        // Before fix, native plan had probe = RemoteSource (no HASH exchange), build = HASH,
        // causing probe rows to miss build partitions and anti-join to incorrectly return rows.
        // After fix, AddExchanges must add HASH exchange on probe side to ensure co-partitioning,
        // even when probe is SINGLE (from LIMIT subquery).
        Session session = getQueryRunner().getDefaultSession();

        String sql = "SELECT COUNT(*) FROM (SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (50) WHERE orderkey IS NOT NULL LIMIT 1000) first " +
                "WHERE NOT EXISTS (SELECT 1 FROM orders second WHERE second.orderkey = first.orderkey)";

        PlanNode optimized = getOptimizedPlan(sql, session);

        assertTrue(containsNode(optimized, JoinNode.class) || containsNode(optimized, SemiJoinNode.class),
                "Anti-join should be planned as Join or SemiJoin");

        JoinNode join = findFirst(optimized, JoinNode.class);
        if (join != null) {
            PlanNode probe = join.getType() == JoinType.RIGHT ? join.getRight() : join.getLeft();
            assertTrue(containsHashExchange(probe),
                    "Probe side of partitioned anti-join must have HASH exchange for co-partitioning with build side. " +
                            "Missing exchange caused Bernoulli probe not co-partitioned, returning non-zero instead of 0");
        }
        else {
            SemiJoinNode semiJoin = findFirst(optimized, SemiJoinNode.class);
            assertTrue(semiJoin != null, "Should have SemiJoin if not Join");
            assertTrue(containsHashExchange(semiJoin.getSource()) || containsHashExchange(semiJoin.getFilteringSource()),
                    "SemiJoin sides must be co-partitioned for PARTITIONED distribution");
        }
    }
}
