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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_PAYLOAD_JOINS;
import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPayloadJoinOptimizer
        extends BasePlanTest
{
    public TestPayloadJoinOptimizer()
    {
        super(ImmutableMap.of(
                REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "false"));
    }

    private Session optimizedSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_PAYLOAD_JOINS, "true")
                .build();
    }

    private Session unoptimizedSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_PAYLOAD_JOINS, "false")
                .build();
    }

    @Test
    public void testBasicPayloadJoinRewrite()
    {
        // A chain of 2+ LOJs should produce an AggregationNode (DISTINCT keys)
        String sql = "SELECT l.* FROM lineitem l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN part p ON l.partkey = p.partkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());
        assertFalse(searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Optimized 2-LOJ chain should contain AggregationNode for DISTINCT keys");

        Plan unoptimizedPlan = plan(sql, OPTIMIZED, true, unoptimizedSession());
        assertTrue(searchFrom(unoptimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Unoptimized plan should not contain AggregationNode");
    }

    @Test
    public void testSingleLeftJoinNotRewritten()
    {
        // A single LOJ should NOT trigger the payload join optimization (requires 2+)
        String sql = "SELECT l.* FROM lineitem l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());
        assertTrue(searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Single LOJ should not trigger payload join rewrite");
    }

    @Test
    public void testNonNullKeysReduceProjections()
    {
        // When join keys have IS NOT NULL predicates, the rejoin uses direct equality
        // instead of IS_NULL + COALESCE pairs, producing fewer ProjectNodes.
        String sqlNonNull = "SELECT l.* FROM " +
                "(SELECT * FROM lineitem WHERE orderkey IS NOT NULL AND partkey IS NOT NULL) l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN part p ON l.partkey = p.partkey";

        String sqlNullable = "SELECT l.* FROM lineitem l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN part p ON l.partkey = p.partkey";

        Plan nonNullPlan = plan(sqlNonNull, OPTIMIZED, true, optimizedSession());
        Plan nullablePlan = plan(sqlNullable, OPTIMIZED, true, optimizedSession());

        // Both should be rewritten
        assertFalse(searchFrom(nonNullPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Non-null key query should be rewritten");
        assertFalse(searchFrom(nullablePlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Nullable key query should be rewritten");

        // Non-null plan should have fewer ProjectNodes (no IS_NULL projections)
        int nonNullProjects = searchFrom(nonNullPlan.getRoot())
                .where(n -> n instanceof ProjectNode)
                .findAll().size();
        int nullableProjects = searchFrom(nullablePlan.getRoot())
                .where(n -> n instanceof ProjectNode)
                .findAll().size();
        assertTrue(nonNullProjects < nullableProjects,
                "Non-null plan should have fewer ProjectNodes. " +
                        "Non-null: " + nonNullProjects + ", Nullable: " + nullableProjects);
    }

    @Test
    public void testPartialNonNullKeys()
    {
        // Only one key is non-null — should still be rewritten, with mixed predicate styles
        String sql = "SELECT l.* FROM " +
                "(SELECT * FROM lineitem WHERE orderkey IS NOT NULL) l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN part p ON l.partkey = p.partkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());
        assertFalse(searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Partial non-null key query should be rewritten");
    }

    @Test
    public void testInterveningIdentityProjection()
    {
        // Subquery wrapping generates an identity ProjectNode between LOJs.
        // The pre-pass should remove it so the full chain is optimized.
        String sql = "SELECT sub.*, s.name as s_name FROM " +
                "(SELECT l.orderkey, l.partkey, l.suppkey, o.orderstatus " +
                "FROM lineitem l LEFT JOIN orders o ON l.orderkey = o.orderkey) sub " +
                "LEFT JOIN supplier s ON sub.suppkey = s.suppkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());
        assertFalse(searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Chain with identity projection should be rewritten");
    }

    @Test
    public void testCrossJoinHoisted()
    {
        // Cross join between LOJs should be hoisted above the chain
        String sql = "SELECT l.orderkey, l.partkey, o.orderstatus, n.name as nation_name, p.brand " +
                "FROM lineitem l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "CROSS JOIN (SELECT name FROM nation WHERE name = 'JAPAN') n " +
                "LEFT JOIN part p ON l.partkey = p.partkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());
        assertFalse(searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Chain with cross join should be rewritten after hoisting");
    }

    @Test
    public void testAllBaseKeyedJoinsOptimized()
    {
        // When all LOJs in the chain are base-keyed, the full chain is optimized
        String sql = "SELECT l.orderkey, l.partkey, l.suppkey, " +
                "o.orderstatus, p.brand, s.name " +
                "FROM lineitem l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN part p ON l.partkey = p.partkey " +
                "LEFT JOIN supplier s ON l.suppkey = s.suppkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());
        assertFalse(searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "All-base-keyed 3-LOJ chain should be rewritten");
    }

    @Test
    public void testDependentJoinKeyFromRightSideAbortsRewrite()
    {
        // When a join key comes from the right side of a prior LOJ (dependent LOJ),
        // the optimizer correctly handles the chain. The 2-LOJ chain with a dependent
        // second LOJ may or may not be optimized depending on key provenance.
        String sql = "SELECT l.orderkey, o.orderstatus, s.name " +
                "FROM lineitem l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN supplier s ON o.shippriority = s.suppkey";

        // This query has 2 LOJs but the second one uses o.shippriority (from orders RHS).
        // The optimizer aborts when it detects collected join keys in the RHS.
        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());
        Plan unoptimizedPlan = plan(sql, OPTIMIZED, true, unoptimizedSession());

        // Both plans should have the same number of AggregationNodes (none),
        // confirming the optimizer correctly handles this case without crashing
        assertEquals(
                searchFrom(optimizedPlan.getRoot())
                        .where(n -> n instanceof AggregationNode)
                        .findAll().size(),
                searchFrom(unoptimizedPlan.getRoot())
                        .where(n -> n instanceof AggregationNode)
                        .findAll().size(),
                "Dependent join key chain should produce same aggregation structure");
    }

    @Test
    public void testNonIdentityProjectionPreservesChain()
    {
        // Non-identity projection computing a join key should not break the chain
        String sql = "SELECT sub.*, s.name as s_name FROM " +
                "(SELECT l.orderkey, l.partkey, l.suppkey + 0 as sk, o.orderstatus " +
                "FROM lineitem l LEFT JOIN orders o ON l.orderkey = o.orderkey) sub " +
                "LEFT JOIN supplier s ON sub.sk = s.suppkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());
        assertFalse(searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty(),
                "Non-identity projection with computed join key should be rewritten");
    }

    @Test
    public void testAllNonNullKeysUseInnerJoinRejoin()
    {
        // When all join keys are guaranteed non-null, the rejoin should use INNER join
        String sql = "SELECT l.* FROM " +
                "(SELECT * FROM lineitem WHERE orderkey IS NOT NULL AND partkey IS NOT NULL) l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN part p ON l.partkey = p.partkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());

        // The rejoin JoinNode should be INNER (payload is left side, so it's the top-most join
        // that has a child AggregationNode on the right side)
        boolean hasInnerRejoin = searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof JoinNode && ((JoinNode) n).getType() == JoinType.INNER)
                .findAll().stream()
                .anyMatch(n -> !searchFrom(((JoinNode) n).getRight())
                        .where(c -> c instanceof AggregationNode)
                        .findAll().isEmpty());
        assertTrue(hasInnerRejoin,
                "All non-null keys should produce INNER join rejoin");
    }

    @Test
    public void testNullableKeysUseLeftJoinRejoin()
    {
        // When keys are nullable, the rejoin should remain LEFT join
        String sql = "SELECT l.* FROM lineitem l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN part p ON l.partkey = p.partkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());

        // The rejoin JoinNode should be LEFT
        boolean hasLeftRejoin = searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof JoinNode && ((JoinNode) n).getType() == JoinType.LEFT)
                .findAll().stream()
                .anyMatch(n -> !searchFrom(((JoinNode) n).getRight())
                        .where(c -> c instanceof AggregationNode)
                        .findAll().isEmpty());
        assertTrue(hasLeftRejoin,
                "Nullable keys should produce LEFT join rejoin");
    }

    @Test
    public void testPartialNonNullKeysUseLeftJoinRejoin()
    {
        // When only some keys are non-null, the rejoin should remain LEFT join
        String sql = "SELECT l.* FROM " +
                "(SELECT * FROM lineitem WHERE orderkey IS NOT NULL) l " +
                "LEFT JOIN orders o ON l.orderkey = o.orderkey " +
                "LEFT JOIN part p ON l.partkey = p.partkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());

        // The rejoin should still be LEFT since partkey might be null
        boolean hasLeftRejoin = searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof JoinNode && ((JoinNode) n).getType() == JoinType.LEFT)
                .findAll().stream()
                .anyMatch(n -> !searchFrom(((JoinNode) n).getRight())
                        .where(c -> c instanceof AggregationNode)
                        .findAll().isEmpty());
        assertTrue(hasLeftRejoin,
                "Partial non-null keys should produce LEFT join rejoin");
    }

    @Test
    public void testNonDeterministicKeysUseLeftJoinRejoin()
    {
        // Even if keys are non-null, non-deterministic computed keys should use LEFT join
        // because the cloned payload and distinct-keys subtrees would compute different values
        String sql = "SELECT sub.* FROM " +
                "(SELECT l.orderkey, l.partkey, CAST(random() * 100 AS BIGINT) + l.suppkey as rk " +
                "FROM lineitem l WHERE orderkey IS NOT NULL AND partkey IS NOT NULL) sub " +
                "LEFT JOIN orders o ON sub.orderkey = o.orderkey " +
                "LEFT JOIN supplier s ON sub.rk = s.suppkey";

        Plan optimizedPlan = plan(sql, OPTIMIZED, true, optimizedSession());

        // If optimized, the rejoin should be LEFT (not INNER) due to non-deterministic key
        boolean hasAggregation = !searchFrom(optimizedPlan.getRoot())
                .where(n -> n instanceof AggregationNode)
                .findAll().isEmpty();

        if (hasAggregation) {
            // Only check rejoin type if the optimizer actually fired
            boolean hasInnerRejoin = searchFrom(optimizedPlan.getRoot())
                    .where(n -> n instanceof JoinNode && ((JoinNode) n).getType() == JoinType.INNER)
                    .findAll().stream()
                    .anyMatch(n -> !searchFrom(((JoinNode) n).getRight())
                            .where(c -> c instanceof AggregationNode)
                            .findAll().isEmpty());
            assertFalse(hasInnerRejoin,
                    "Non-deterministic key should prevent INNER join rejoin");
        }
    }
}
