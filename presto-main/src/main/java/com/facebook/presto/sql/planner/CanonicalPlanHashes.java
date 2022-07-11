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

import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CanonicalPlanHashes
{
    private final Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> hashes;

    public CanonicalPlanHashes(PlanNode root, ObjectMapper objectMapper)
    {
        Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> planHashes = new HashMap<>();
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            CanonicalPlanGenerator.Context context = new CanonicalPlanGenerator.Context();
            root.accept(new CanonicalPlanGenerator(strategy), context);

            for (Map.Entry<PlanNodeId, CanonicalPlan> entry : context.getCanonicalPlans().entrySet()) {
                try {
                    String hash = sha256().hashString(objectMapper.writeValueAsString(entry.getValue()), UTF_8).toString();
                    planHashes.computeIfAbsent(entry.getKey(), ignored -> new HashMap<>()).put(strategy, hash);
                }
                catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        this.hashes = ImmutableMap.copyOf(planHashes);
    }

    public Map<PlanCanonicalizationStrategy, String> getCanonicalPlanHashes(PlanNodeId id)
    {
        return hashes.getOrDefault(id, ImmutableMap.of());
    }

    @VisibleForTesting
    public List<String> getAllHashes(PlanCanonicalizationStrategy strategy)
    {
        return hashes.values().stream()
                .filter(map -> map.containsKey(strategy))
                .map(map -> map.get(strategy))
                .collect(toImmutableList());
    }
}
