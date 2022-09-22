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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.google.common.graph.Traverser.forTree;
import static java.util.Objects.requireNonNull;

public class PlanNodeCanonicalInfo
{
    private final String hash;
    private final List<PlanStatistics> inputTableStatistics;

    @JsonCreator
    public PlanNodeCanonicalInfo(@JsonProperty("hash") String hash, @JsonProperty("inputTableStatistics") List<PlanStatistics> inputTableStatistics)
    {
        this.hash = requireNonNull(hash, "hash is null");
        this.inputTableStatistics = requireNonNull(inputTableStatistics, "inputTableStatistics is null");
    }

    @JsonProperty
    public String getHash()
    {
        return hash;
    }

    @JsonProperty
    public List<PlanStatistics> getInputTableStatistics()
    {
        return inputTableStatistics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanNodeCanonicalInfo that = (PlanNodeCanonicalInfo) o;
        return hash == that.hash && inputTableStatistics.equals(that.inputTableStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(System.identityHashCode(hash), inputTableStatistics);
    }

    public static List<CanonicalPlanWithInfo> getCanonicalInfo(
            Session session,
            PlanNode root,
            PlanCanonicalInfoProvider planCanonicalInfoProvider)
    {
        ImmutableList.Builder<CanonicalPlanWithInfo> result = ImmutableList.builder();
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            for (PlanNode node : forTree(PlanNode::getSources).depthFirstPreOrder(root)) {
                if (!node.getStatsEquivalentPlanNode().isPresent()) {
                    continue;
                }
                PlanNode statsEquivalentPlanNode = node.getStatsEquivalentPlanNode().get();
                Optional<String> hash = planCanonicalInfoProvider.hash(session, statsEquivalentPlanNode, strategy);
                Optional<List<PlanStatistics>> inputTableStatistics = planCanonicalInfoProvider.getInputTableStatistics(session, statsEquivalentPlanNode);
                if (hash.isPresent() && inputTableStatistics.isPresent()) {
                    result.add(new CanonicalPlanWithInfo(new CanonicalPlan(statsEquivalentPlanNode, strategy), new PlanNodeCanonicalInfo(hash.get(), inputTableStatistics.get())));
                }
            }
        }
        return result.build();
    }
}
