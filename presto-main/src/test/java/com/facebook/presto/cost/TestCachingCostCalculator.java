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

package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.DummyMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestCachingCostCalculator
{
    private final Session session = TestingSession.testSessionBuilder().build();
    private final Map types = emptyMap();

    @Test
    public void test()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), new DummyMetadata());

        ValuesNode values = planBuilder.values();
        PlanNode project = planBuilder.project(
                Assignments.identity(),
                planBuilder.project(
                        Assignments.identity(),
                        values));

        CostCalculator costCalculator = new CachingCostCalculator(new DummyCostCalculator());
        Lookup lookup = Lookup.from(node -> node, (node, lookup1, session, types) -> null, costCalculator);

        assertEquals(costCalculator.calculateCost(project, lookup, session, types), cpuCost(1.0));
        assertEquals(costCalculator.calculateCost(project, lookup, session, types), cpuCost(1.0));

        assertEquals(costCalculator.calculateCost(values, lookup, session, types), cpuCost(10.0));
        assertEquals(costCalculator.calculateCost(values, lookup, session, types), cpuCost(10.0));

        assertEquals(costCalculator.calculateCumulativeCost(project, lookup, session, types), cpuCost(12.0));
        assertEquals(costCalculator.calculateCumulativeCost(project, lookup, session, types), cpuCost(12.0));

        assertEquals(costCalculator.calculateCost(values, lookup, session, types), cpuCost(10.0));
        assertEquals(costCalculator.calculateCost(values, lookup, session, types), cpuCost(10.0));
    }

    private static class DummyCostCalculator
            implements CostCalculator
    {
        @Override
        public PlanNodeCostEstimate calculateCost(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
        {
            if (planNode instanceof ProjectNode) {
                return cpuCost(1);
            }
            else if (planNode instanceof ValuesNode) {
                return cpuCost(10);
            }
            throw new UnsupportedOperationException();
        }
    }
}
