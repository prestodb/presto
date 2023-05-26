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

import com.facebook.presto.Session;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.encoding.StringUtils.UTF_8;
import static com.google.common.hash.Hashing.sha256;

public class PlanAnalyticsInfoProvider
        implements PlanCanonicalInfoProvider
{
    private final ObjectMapper objectMapper;

    public PlanAnalyticsInfoProvider(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<String> hash(Session session, PlanNode planNode, PlanCanonicalizationStrategy strategy)
    {
        return hash(session, planNode, strategy, new PlanAnalyticsInfoContext());
    }

    public Optional<String> hash(Session session, PlanNode planNode, PlanCanonicalizationStrategy strategy, PlanAnalyticsInfoContext planAnalyticsInfoContext)
    {
        Map<PlanNode, String> perQueryCacheMap = planAnalyticsInfoContext.getPerQueryPlanNodeMap();
        if (perQueryCacheMap.containsKey(planNode)) {
            return Optional.of(perQueryCacheMap.get(planNode));
        }
        CanonicalPlanGenerator.Context context = new CanonicalPlanGenerator.Context();
        planNode.accept(
                new CanonicalPlanGenerator(PlanCanonicalizationStrategy.EXACT, objectMapper, session),
                context);
        context
                .getCanonicalPlans()
                .forEach(
                        (p, canonicalPlan) -> {
                            perQueryCacheMap.put(p, hashCanonicalPlan(canonicalPlan, objectMapper));
                        });
        return Optional.ofNullable(perQueryCacheMap.getOrDefault(planNode, null));
    }

    @Override
    public Optional<List<PlanStatistics>> getInputTableStatistics(Session session, PlanNode planNode)
    {
        return Optional.empty();
    }

    private String hashCanonicalPlan(CanonicalPlan plan, ObjectMapper objectMapper)
    {
        return sha256().hashString(plan.toString(objectMapper), UTF_8).toString();
    }

    public static class PlanAnalyticsInfoContext
    {
        private final Map<PlanNode, String> perQueryPlanNodeMap;

        public PlanAnalyticsInfoContext()
        {
            this.perQueryPlanNodeMap = new HashMap<>();
        }

        public Map<PlanNode, String> getPerQueryPlanNodeMap()
        {
            return perQueryPlanNodeMap;
        }
    }
}
