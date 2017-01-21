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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestQuantifiedComparison
        extends BasePlanTest
{
    @Test
    public void testQuantifiedComparisonEqualsAny()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey = ANY (VALUES ROW(CAST(5 as BIGINT)), ROW(CAST(3 as BIGINT)))";
        assertPlan(query, anyTree(
                filter("S",
                        project(
                                semiJoin("X", "Y", "S",
                                        anyTree(tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(values(ImmutableMap.of("Y", 0))))))));
    }

    @Test
    public void testQuantifiedComparisonNotEqualsAll()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey <> ALL (VALUES ROW(CAST(5 as BIGINT)), ROW(CAST(3 as BIGINT)))";
        assertPlan(query, anyTree(
                filter("NOT S",
                        project(
                                semiJoin("X", "Y", "S",
                                        anyTree(tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(values(ImmutableMap.of("Y", 0))))))));
    }

    @Test
    public void testQuantifiedComparisonLessAll()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey < ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonGreaterEqualAll()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey >= ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonLessSome()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey < SOME (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonGreaterEqualAny()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey >= ANY (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonEqualAll()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey = ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonNotEqualAny()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey <> SOME (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    private void assertQuantifiedComparison(String query)
    {
        assertPlan(query, anyTree(
                node(JoinNode.class,
                        tableScan("orders"),
                        anyTree(
                                node(AggregationNode.class,
                                        node(ValuesNode.class))))));
    }
}
