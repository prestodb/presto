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
package com.facebook.presto.sql.query;

import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static org.testng.Assert.assertTrue;

public class TestPreAggregateCountThroughOuterJoinSemantics
{
    @Test
    public void testLeftJoinCountWithDuplicatesAndUnmatchedRows()
    {
        try (QueryAssertions assertions = new QueryAssertions(ImmutableMap.of(PUSH_AGGREGATION_THROUGH_JOIN, "true"))) {
            assertQueryAndPreAggregatedOuterJoin(
                    assertions,
                    "WITH " +
                            "outer_relation(group_key, join_key) AS (VALUES (1, 10), (1, 10), (1, 20), (2, 30), (3, 40)), " +
                            "inner_relation(join_key, value) AS (VALUES (10, 'a'), (10, 'b'), (20, NULL), (20, 'c'), (30, NULL), (50, 'x')) " +
                            "SELECT group_key, count(value) " +
                            "FROM outer_relation " +
                            "LEFT JOIN inner_relation ON outer_relation.join_key = inner_relation.join_key " +
                            "GROUP BY group_key",
                    "VALUES (1, BIGINT '5'), (2, BIGINT '0'), (3, BIGINT '0')",
                    LEFT);
        }
    }

    @Test
    public void testRightJoinCountWithDuplicatesAndUnmatchedRows()
    {
        try (QueryAssertions assertions = new QueryAssertions(ImmutableMap.of(PUSH_AGGREGATION_THROUGH_JOIN, "true"))) {
            assertQueryAndPreAggregatedOuterJoin(
                    assertions,
                    "WITH " +
                            "outer_relation(group_key, join_key) AS (VALUES (1, 10), (1, 10), (1, 20), (2, 30), (3, 40)), " +
                            "inner_relation(join_key, value) AS (VALUES (10, 'a'), (10, 'b'), (20, NULL), (20, 'c'), (30, NULL), (50, 'x')) " +
                            "SELECT group_key, count(value) " +
                            "FROM inner_relation " +
                            "RIGHT JOIN outer_relation ON inner_relation.join_key = outer_relation.join_key " +
                            "GROUP BY group_key",
                    "VALUES (1, BIGINT '5'), (2, BIGINT '0'), (3, BIGINT '0')",
                    RIGHT);
        }
    }

    @Test
    public void testGlobalCountWithEmptyPreservedSide()
    {
        try (QueryAssertions assertions = new QueryAssertions(ImmutableMap.of(PUSH_AGGREGATION_THROUGH_JOIN, "true"))) {
            assertions.assertQuery(
                    "WITH " +
                            "outer_relation(join_key) AS (SELECT * FROM (VALUES 1) WHERE false), " +
                            "inner_relation(join_key, value) AS (VALUES (1, 'a'), (1, 'b')) " +
                            "SELECT count(value) " +
                            "FROM outer_relation " +
                            "LEFT JOIN inner_relation ON outer_relation.join_key = inner_relation.join_key",
                    "VALUES BIGINT '0'");
        }
    }

    private static void assertQueryAndPreAggregatedOuterJoin(QueryAssertions assertions, String actual, String expected, JoinType joinType)
    {
        assertions.assertQuery(actual, expected);
        Plan plan = assertions.getQueryRunner().createPlan(assertions.getQueryRunner().getDefaultSession(), actual, WarningCollector.NOOP);
        assertContainsPreAggregatedOuterJoin(plan, joinType);
    }

    private static void assertContainsPreAggregatedOuterJoin(Plan plan, JoinType joinType)
    {
        assertTrue(
                containsPreAggregatedOuterJoin(plan.getRoot(), joinType),
                "Expected plan to contain pre-aggregated " + joinType + " join");
    }

    private static boolean containsPreAggregatedOuterJoin(PlanNode node, JoinType joinType)
    {
        if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            if (join.getType() == joinType) {
                PlanNode inner = joinType == LEFT ? join.getRight() : join.getLeft();
                if (containsNode(inner, AggregationNode.class)) {
                    return true;
                }
            }
        }

        return node.getSources().stream()
                .anyMatch(source -> containsPreAggregatedOuterJoin(source, joinType));
    }

    private static boolean containsNode(PlanNode node, Class<? extends PlanNode> nodeClass)
    {
        if (nodeClass.isInstance(node)) {
            return true;
        }

        return node.getSources().stream()
                .anyMatch(source -> containsNode(source, nodeClass));
    }
}
