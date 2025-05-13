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
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.SystemSessionProperties.HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY;
import static com.facebook.presto.cost.HistoryBasedPlanStatisticsManager.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestHistoryBasedOptimizationConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HistoryBasedOptimizationConfig.class)
                .setMaxLastRunsHistory(10)
                .setHistoryMatchingThreshold(0.1));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hbo.max-last-runs-history", "50")
                .put("hbo.history-matching-threshold", "0.2")
                .build();

        HistoryBasedOptimizationConfig expected = new HistoryBasedOptimizationConfig()
                .setMaxLastRunsHistory(50)
                .setHistoryMatchingThreshold(0.2);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testPlanCanonicalizationStrategyOrder()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SAFE_CONSTANTS,DEFAULT,CONNECTOR,IGNORE_SCAN_CONSTANTS")
                .build();
        List<PlanCanonicalizationStrategy> strategyList = historyBasedPlanCanonicalizationStrategyList(session);
        assertEquals(strategyList.size(), 4);
        assertEquals(strategyList.get(0), PlanCanonicalizationStrategy.DEFAULT);
        assertEquals(strategyList.get(1), PlanCanonicalizationStrategy.CONNECTOR);
        assertEquals(strategyList.get(2), PlanCanonicalizationStrategy.IGNORE_SAFE_CONSTANTS);
        assertEquals(strategyList.get(3), PlanCanonicalizationStrategy.IGNORE_SCAN_CONSTANTS);

        session = testSessionBuilder()
                .setSystemProperty(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SAFE_CONSTANTS,IGNORE_SCAN_CONSTANTS")
                .build();
        strategyList = historyBasedPlanCanonicalizationStrategyList(session);
        assertEquals(strategyList.size(), 2);
        assertEquals(strategyList.get(0), PlanCanonicalizationStrategy.IGNORE_SAFE_CONSTANTS);
        assertEquals(strategyList.get(1), PlanCanonicalizationStrategy.IGNORE_SCAN_CONSTANTS);

        session = testSessionBuilder()
                .setSystemProperty(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SCAN_CONSTANTS,IGNORE_SAFE_CONSTANTS")
                .build();
        strategyList = historyBasedPlanCanonicalizationStrategyList(session);
        assertEquals(strategyList.size(), 2);
        assertEquals(strategyList.get(0), PlanCanonicalizationStrategy.IGNORE_SAFE_CONSTANTS);
        assertEquals(strategyList.get(1), PlanCanonicalizationStrategy.IGNORE_SCAN_CONSTANTS);
    }
}
