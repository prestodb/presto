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
import org.testng.annotations.Test;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.ZERO_COST;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestCostComparator
{
    @Test
    public void testCpuWeight()
    {
        new CostComparisonAssertion(1.0, 0.0, 0.0)
                .smaller(200, 200, 200)
                .larger(1000, 100, 100)
                .assertCompare();
    }

    @Test
    public void testMemoryWeight()
    {
        new CostComparisonAssertion(0.0, 1.0, 0.0)
                .smaller(200, 200, 200)
                .larger(100, 1000, 100)
                .assertCompare();
    }

    @Test
    public void testNetworkWeight()
    {
        new CostComparisonAssertion(0.0, 0.0, 1.0)
                .smaller(200, 200, 200)
                .larger(100, 100, 1000)
                .assertCompare();
    }

    @Test
    public void testAllWeights()
    {
        new CostComparisonAssertion(1.0, 1.0, 1.0)
                .smaller(333, 333, 333)
                .larger(200, 300, 500)
                .assertCompare();

        new CostComparisonAssertion(1.0, 1000.0, 1.0)
                .smaller(300, 299, 300)
                .larger(100, 300, 100)
                .assertCompare();
    }

    @Test
    public void testUnknownCost()
    {
        CostComparator costComparator = new CostComparator(1.0, 1.0, 1.0);
        Session session = testSessionBuilder().build();
        assertThrows(IllegalArgumentException.class, () -> costComparator.compare(session, ZERO_COST, UNKNOWN_COST));
        assertThrows(IllegalArgumentException.class, () -> costComparator.compare(session, UNKNOWN_COST, ZERO_COST));
        assertThrows(IllegalArgumentException.class, () -> costComparator.compare(session, UNKNOWN_COST, UNKNOWN_COST));
    }

    private static class CostComparisonAssertion
    {
        private final PlanNodeCostEstimate.Builder smaller = PlanNodeCostEstimate.builder();
        private final PlanNodeCostEstimate.Builder larger = PlanNodeCostEstimate.builder();
        private final CostComparator costComparator;
        private Session session = testSessionBuilder().build();

        public CostComparisonAssertion(double cpuWeight, double memoryWeight, double networkWeight)
        {
            costComparator = new CostComparator(cpuWeight, memoryWeight, networkWeight);
        }

        public void assertCompare()
        {
            assertTrue(costComparator.compare(session, smaller.build(), larger.build()) < 0,
                    "smaller < larger is false");

            assertTrue(costComparator.compare(session, larger.build(), smaller.build()) > 0,
                    "larger > smaller is false");
        }

        public CostComparisonAssertion smaller(double cpu, double memory, double network)
        {
            smaller.setCpuCost(cpu).setMemoryCost(memory).setNetworkCost(network);
            return this;
        }

        public CostComparisonAssertion larger(double cpu, double memory, double network)
        {
            larger.setCpuCost(cpu).setMemoryCost(memory).setNetworkCost(network);
            return this;
        }

        public CostComparisonAssertion session(Session session)
        {
            this.session = session;
            return this;
        }
    }
}
