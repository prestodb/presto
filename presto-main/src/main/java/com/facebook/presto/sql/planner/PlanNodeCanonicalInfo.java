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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.cost.HistoryBasedPlanStatisticsManager.historyBasedPlanCanonicalizationStrategyList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.graph.Traverser.forTree;
import static java.util.Objects.requireNonNull;

public class PlanNodeCanonicalInfo
{
    private final String hash;
    private final Optional<List<PlanStatistics>> inputTableStatistics;
    private final Optional<List<InputTableCacheKey>> inputTableCacheKeys;

    @JsonCreator
    public PlanNodeCanonicalInfo(@JsonProperty("hash") String hash,
            @JsonProperty("inputTableStatistics") Optional<List<PlanStatistics>> inputTableStatistics,
            @JsonProperty("inputTableCacheKeys") Optional<List<InputTableCacheKey>> inputTableCacheKeys)
    {
        checkArgument(inputTableStatistics.isPresent() || inputTableCacheKeys.isPresent(), "inputTableStatistics and inputTableCacheKeys should not both be empty");
        this.hash = requireNonNull(hash, "hash is null");
        this.inputTableStatistics = requireNonNull(inputTableStatistics, "inputTableStatistics is null");
        this.inputTableCacheKeys = requireNonNull(inputTableCacheKeys, "inputTableCacheKeys is null");
    }

    @JsonProperty
    public String getHash()
    {
        return hash;
    }

    @JsonProperty
    public Optional<List<PlanStatistics>> getInputTableStatistics()
    {
        return inputTableStatistics;
    }

    @JsonProperty
    public Optional<List<InputTableCacheKey>> getInputTableCacheKeys()
    {
        return inputTableCacheKeys;
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
        return hash.equals(that.hash) && inputTableStatistics.equals(that.inputTableStatistics) && inputTableCacheKeys.equals(that.inputTableCacheKeys);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(System.identityHashCode(hash), inputTableStatistics, inputTableCacheKeys);
    }

    public static List<CanonicalPlanWithInfo> getCanonicalInfo(
            Session session,
            PlanNode root,
            PlanCanonicalInfoProvider planCanonicalInfoProvider)
    {
        ImmutableList.Builder<CanonicalPlanWithInfo> result = ImmutableList.builder();
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList(session)) {
            for (PlanNode node : forTree(PlanNode::getSources).depthFirstPreOrder(root)) {
                if (!node.getStatsEquivalentPlanNode().isPresent()) {
                    continue;
                }
                PlanNode statsEquivalentPlanNode = node.getStatsEquivalentPlanNode().get();
                Optional<String> hash = planCanonicalInfoProvider.hash(session, statsEquivalentPlanNode, strategy, true);
                Optional<List<PlanStatistics>> inputTableStatistics = planCanonicalInfoProvider.getInputTableStatistics(session, statsEquivalentPlanNode, strategy, false, true);
                Optional<List<InputTableCacheKey>> inputTableCacheKeyList = planCanonicalInfoProvider.getInputTableCacheKey(session, statsEquivalentPlanNode, strategy, true);
                if (hash.isPresent()) {
                    result.add(new CanonicalPlanWithInfo(new CanonicalPlan(statsEquivalentPlanNode, strategy), new PlanNodeCanonicalInfo(hash.get(), inputTableStatistics, inputTableCacheKeyList)));
                }
            }
        }
        return result.build();
    }

    public static class InputTableCacheKey
    {
        private final TableHandle tableHandle;
        private final List<ColumnHandle> columnHandles;
        private final Constraint<ColumnHandle> constraint;

        public InputTableCacheKey(TableHandle tableHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.columnHandles = ImmutableList.copyOf(columnHandles);
            this.constraint = requireNonNull(constraint, "constraint is null");
        }

        public TableHandle getTableHandle()
        {
            return tableHandle;
        }

        public List<ColumnHandle> getColumnHandles()
        {
            return columnHandles;
        }

        public Constraint<ColumnHandle> getConstraint()
        {
            return constraint;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof InputTableCacheKey)) {
                return false;
            }

            InputTableCacheKey other = (InputTableCacheKey) obj;
            return this.tableHandle.equals(other.tableHandle) && this.columnHandles.equals(other.columnHandles) && this.constraint.equals(other.constraint);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableHandle, columnHandles, constraint);
        }
    }
}
