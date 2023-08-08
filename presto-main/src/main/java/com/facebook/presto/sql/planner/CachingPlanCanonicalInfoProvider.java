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
import com.facebook.presto.cost.HistoryBasedStatisticsCacheManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CachingPlanCanonicalInfoProvider
        implements PlanCanonicalInfoProvider
{
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    private final ObjectMapper objectMapper;
    private final Metadata metadata;

    public CachingPlanCanonicalInfoProvider(HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager, ObjectMapper objectMapper, Metadata metadata)
    {
        this.historyBasedStatisticsCacheManager = requireNonNull(historyBasedStatisticsCacheManager, "historyBasedStatisticsCacheManager is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Optional<String> hash(Session session, PlanNode planNode, PlanCanonicalizationStrategy strategy)
    {
        CacheKey key = new CacheKey(planNode, strategy);
        return loadValue(session, key).map(PlanNodeCanonicalInfo::getHash);
    }

    @Override
    public Optional<List<PlanStatistics>> getInputTableStatistics(Session session, PlanNode planNode)
    {
        CacheKey key = new CacheKey(planNode, historyBasedPlanCanonicalizationStrategyList().get(0));
        return loadValue(session, key).map(PlanNodeCanonicalInfo::getInputTableStatistics);
    }

    private Optional<PlanNodeCanonicalInfo> loadValue(Session session, CacheKey key)
    {
        Map<CacheKey, PlanNodeCanonicalInfo> cache = historyBasedStatisticsCacheManager.getCanonicalInfoCache(session.getQueryId());
        PlanNodeCanonicalInfo result = cache.get(key);
        if (result != null) {
            return Optional.of(result);
        }
        CanonicalPlanGenerator.Context context = new CanonicalPlanGenerator.Context();
        key.getNode().accept(new CanonicalPlanGenerator(key.getStrategy(), objectMapper, session), context);
        context.getCanonicalPlans().forEach((plan, canonicalPlan) -> {
            String hashValue = hashCanonicalPlan(canonicalPlan, objectMapper);
            // Compute input table statistics for the plan node. This is useful in history based optimizations,
            // where historical plan statistics are reused if input tables are similar in size across runs.
            List<PlanStatistics> inputTableStatistics = context.getInputTables().get(plan).stream()
                    .map(table -> getPlanStatisticsForTable(session, table))
                    .collect(toImmutableList());
            cache.put(new CacheKey(plan, key.getStrategy()), new PlanNodeCanonicalInfo(hashValue, inputTableStatistics));
        });
        return Optional.ofNullable(cache.get(key));
    }

    private PlanStatistics getPlanStatisticsForTable(Session session, TableScanNode table)
    {
        InputTableCacheKey key = new InputTableCacheKey(new TableHandle(
                table.getTable().getConnectorId(),
                table.getTable().getConnectorHandle(),
                table.getTable().getTransaction(),
                Optional.empty()), ImmutableList.copyOf(table.getAssignments().values()), new Constraint<>(table.getCurrentConstraint()));
        Map<InputTableCacheKey, PlanStatistics> cache = historyBasedStatisticsCacheManager.getInputTableStatistics(session.getQueryId());
        PlanStatistics planStatistics = cache.get(key);
        if (planStatistics != null) {
            return planStatistics;
        }
        TableStatistics tableStatistics = metadata.getTableStatistics(session, key.getTableHandle(), key.getColumnHandles(), key.getConstraint());
        planStatistics = new PlanStatistics(tableStatistics.getRowCount(), tableStatistics.getTotalSize(), 1);
        cache.put(key, planStatistics);
        return planStatistics;
    }

    @VisibleForTesting
    public long getCacheSize()
    {
        return historyBasedStatisticsCacheManager.getCanonicalInfoCache().values().stream().mapToLong(cache -> cache.size()).sum();
    }

    @VisibleForTesting
    public HistoryBasedStatisticsCacheManager getHistoryBasedStatisticsCacheManager()
    {
        return historyBasedStatisticsCacheManager;
    }

    private String hashCanonicalPlan(CanonicalPlan plan, ObjectMapper objectMapper)
    {
        return sha256().hashString(plan.toString(objectMapper), UTF_8).toString();
    }

    public static class CacheKey
    {
        private final PlanNode node;
        private final PlanCanonicalizationStrategy strategy;

        public CacheKey(PlanNode node, PlanCanonicalizationStrategy strategy)
        {
            this.node = requireNonNull(node, "node is null");
            this.strategy = requireNonNull(strategy, "strategy is null");
        }

        public PlanNode getNode()
        {
            return node;
        }

        public PlanCanonicalizationStrategy getStrategy()
        {
            return strategy;
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
            CacheKey cacheKey = (CacheKey) o;
            return node == cacheKey.node && strategy.equals(cacheKey.strategy);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(System.identityHashCode(node), strategy);
        }
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
