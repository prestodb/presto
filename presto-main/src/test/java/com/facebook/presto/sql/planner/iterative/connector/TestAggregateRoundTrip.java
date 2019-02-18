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
package com.facebook.presto.sql.planner.iterative.connector;

import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.InlineProjections;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestAggregateRoundTrip
        extends TestPlanNodeTableExpressionRoundTrip
{
    @Test
    public void testAggregateRoundTrip()
    {
        List<PlanOptimizer> optimizeProjections = new ImmutableList.Builder()
                .add(new PruneUnreferencedOutputs())
                .add(new IterativeOptimizer(
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getCostCalculator(),
                        ImmutableSet.of(
                                new InlineProjections(),
                                new RemoveRedundantIdentityProjections())))
                .build();
        assertRoundTrip(AggregationNode.class, "SELECT orderkey,  custkey, approx_distinct(clerk, 0.012) FROM orders WHERE orderkey > 1 \n" +
                " GROUP BY GROUPING SETS ((orderkey, custkey), (orderkey))", this::assertAggregationEquals, optimizeProjections);
        assertRoundTrip(AggregationNode.class, "SELECT orderkey,  custkey, sum(totalprice) FROM orders WHERE orderkey > 1 \n" +
                " GROUP BY GROUPING SETS ((orderkey, custkey), (orderkey))", this::assertAggregationEquals, optimizeProjections);
        assertRoundTrip(AggregationNode.class, "SELECT orderkey +1,  custkey, sum(totalprice) FROM orders WHERE orderkey > 1 GROUP BY 1, 2", this::assertAggregationEquals, optimizeProjections);
        // TODO test equivalence of columnExpression of tableExpression following creates equal columnExpression but not exact match
        // rewritten add CAST( ) to CAST(CAST(12 DECIMAL(3,3))
        assertRoundTrip(AggregationNode.class, "SELECT orderkey +1,  custkey, approx_distinct(clerk, 0.012) FROM orders WHERE orderkey > 1 GROUP BY 1, 2", this::assertAggregationEquals, optimizeProjections);

        assertRoundTripFail(AggregationNode.class, "SELECT orderkey +1,  custkey, sum(totalprice) FILTER (WHERE orderkey > 1) FROM orders WHERE orderkey > 1 GROUP BY 1, 2", this::assertAggregationEquals, optimizeProjections);
        assertRoundTripFail(AggregationNode.class, "SELECT orderkey +1,  COUNT(DISTINCT custkey) FROM orders WHERE orderkey > 1 GROUP BY 1", this::assertAggregationEquals, optimizeProjections);
    }

    private void assertAggregationEquals(AggregationNode actual, AggregationNode expected)
    {
        assertEquals(actual.getOutputSymbols(), expected.getOutputSymbols());
    }
}
