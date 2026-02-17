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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPlanPrinterSmoke
        extends BasePlanTest
{
    @Test
    public void testLogicalPlanTextSizeEstimates()
    {
        String testSql = "SELECT \n" +
                "  *\n" +
                "FROM \n" +
                "  supplier s,\n" +
                "  lineitem l1,\n" +
                "  orders o,\n" +
                "  nation n\n" +
                "WHERE \n" +
                "  s.suppkey = l1.suppkey \n" +
                "  AND o.orderkey = l1.orderkey\n" +
                "  AND s.nationkey = n.nationkey \n" +
                "\n";
        try (LocalQueryRunner localQueryRunner = createQueryRunner(ImmutableMap.of())) {
            localQueryRunner.inTransaction(localQueryRunner.getDefaultSession(), transactionSession -> {
                Plan actualPlan = localQueryRunner.createPlan(
                        transactionSession,
                        testSql,
                        WarningCollector.NOOP);

                StatsProvider statsProvider = new CachingStatsProvider(localQueryRunner.getStatsCalculator(), transactionSession, actualPlan.getTypes());
                CostProvider costProvider = new CachingCostProvider(localQueryRunner.getEstimatedExchangesCostCalculator(), statsProvider, transactionSession);

                String textLogicalPlan = PlanPrinter.textLogicalPlan(actualPlan.getRoot(),
                        actualPlan.getTypes(),
                        StatsAndCosts.create(actualPlan.getRoot(), statsProvider, costProvider, transactionSession),
                        localQueryRunner.getFunctionAndTypeManager(),
                        transactionSession,
                        1);

                // `nation` scan
                assertThat(textLogicalPlan).contains("Estimates: {source: CostBasedSourceInfo, rows: 25 (2.89kB), cpu: 2,734.00, memory: 0.00, network: 0.00}");
                // `supplier` scan
                assertThat(textLogicalPlan).contains("Estimates: {source: CostBasedSourceInfo, rows: 100 (17.14kB), cpu: 16,652.00, memory: 0.00, network: 0.00}");
                // `orders` scan
                assertThat(textLogicalPlan).contains("Estimates: {source: CostBasedSourceInfo, rows: 15,000 (1.99MB), cpu: 1,948,552.00, memory: 0.00, network: 0.00}");
                // `lineitem` scan
                assertThat(textLogicalPlan).contains("Estimates: {source: CostBasedSourceInfo, rows: 60,175 (9.29MB), cpu: 9,197,910.00, memory: 0.00, network: 0.00}");

                // JOINs
                assertThat(textLogicalPlan).contains("Estimates: {source: CostBasedSourceInfo, rows: 60,175 (15.71MB), cpu: 53,349,364.11, memory: 2,083,552.00, network: 2,083,552.00}");
                assertThat(textLogicalPlan).contains("Estimates: {source: CostBasedSourceInfo, rows: 60,175 (30.51MB), cpu: 119,543,090.43, memory: 2,114,099.00, network: 2,114,099.00}");
                assertThat(textLogicalPlan).contains("Estimates: {source: CostBasedSourceInfo, rows: 100 (26.06kB), cpu: 90,055.00, memory: 2,959.00, network: 2,959.00}");

                return null;
            });
        }
    }
}
