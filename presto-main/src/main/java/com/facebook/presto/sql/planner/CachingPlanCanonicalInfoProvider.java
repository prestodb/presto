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
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.JoinNodeStatistics;
import com.facebook.presto.spi.statistics.PartialAggregationStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableWriterNodeStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.enableVerboseHistoryBasedOptimizerRuntimeStats;
import static com.facebook.presto.SystemSessionProperties.getHistoryBasedOptimizerInputStatisticsCheckStrategy;
import static com.facebook.presto.SystemSessionProperties.getHistoryBasedOptimizerTimeoutLimit;
import static com.facebook.presto.SystemSessionProperties.isVerboseRuntimeStatsEnabled;
import static com.facebook.presto.SystemSessionProperties.logQueryPlansUsedInHistoryBasedOptimizer;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.HistoryBasedOptimizerInputStatisticsCheckStrategy.ALWAYS;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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
    public Optional<String> hash(Session session, PlanNode planNode, PlanCanonicalizationStrategy strategy, boolean cacheOnly)
    {
        CacheKey key = new CacheKey(planNode, strategy);
        return loadValue(session, key, cacheOnly).map(PlanNodeCanonicalInfo::getHash);
    }

    @Override
    public Optional<List<PlanStatistics>> getInputTableStatistics(Session session, PlanNode planNode, PlanCanonicalizationStrategy strategy, boolean callMetaData, boolean cacheOnly)
    {
        CacheKey key = new CacheKey(planNode, strategy);
        Optional<PlanNodeCanonicalInfo> planNodeCanonicalInfo = loadValue(session, key, cacheOnly);
        if (planNodeCanonicalInfo.isPresent()) {
            if (planNodeCanonicalInfo.get().getInputTableStatistics().isPresent()) {
                return planNodeCanonicalInfo.get().getInputTableStatistics();
            }
            else if (callMetaData) {
                checkState(planNodeCanonicalInfo.get().getInputTableCacheKeys().isPresent());
                return planNodeCanonicalInfo.get().getInputTableCacheKeys().map(x -> x.stream().map(cacheKey -> getPlanStatisticsForTable(session, cacheKey, false)).collect(toImmutableList()));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<List<PlanNodeCanonicalInfo.InputTableCacheKey>> getInputTableCacheKey(Session session, PlanNode planNode, PlanCanonicalizationStrategy strategy, boolean cacheOnly)
    {
        CacheKey key = new CacheKey(planNode, strategy);
        Optional<PlanNodeCanonicalInfo> planNodeCanonicalInfo = loadValue(session, key, cacheOnly);
        if (planNodeCanonicalInfo.isPresent()) {
            return planNodeCanonicalInfo.get().getInputTableCacheKeys();
        }
        return Optional.empty();
    }

    private Optional<PlanNodeCanonicalInfo> loadValue(Session session, CacheKey key, boolean cacheOnly)
    {
        long startTimeInNano = System.nanoTime();
        long profileStartTime = 0;
        long timeoutInMilliseconds = getHistoryBasedOptimizerTimeoutLimit(session).toMillis();
        boolean enableVerboseRuntimeStats = isVerboseRuntimeStatsEnabled(session) || enableVerboseHistoryBasedOptimizerRuntimeStats(session);
        Map<CacheKey, PlanNodeCanonicalInfo> cache = historyBasedStatisticsCacheManager.getCanonicalInfoCache(session.getQueryId());
        PlanNodeCanonicalInfo result = cache.get(key);
        if (result != null || cacheOnly) {
            return Optional.ofNullable(result);
        }
        CanonicalPlanGenerator.Context context = new CanonicalPlanGenerator.Context();
        if (enableVerboseRuntimeStats) {
            profileStartTime = System.nanoTime();
        }
        key.getNode().accept(new CanonicalPlanGenerator(key.getStrategy(), objectMapper, session), context);
        if (enableVerboseRuntimeStats) {
            profileTime("CanonicalPlanGenerator", profileStartTime, session);
        }
        if (loadValueTimeout(startTimeInNano, timeoutInMilliseconds)) {
            return Optional.empty();
        }
        // Only log the canonicalized plan when the plan node is root node, whose serialized form will include the whole plan
        Optional<PlanNode> statsEquivalentRootNode = historyBasedStatisticsCacheManager.getStatsEquivalentPlanRootNode(session.getQueryId());
        boolean isRootNode = statsEquivalentRootNode.isPresent() && statsEquivalentRootNode.get() == key.getNode();
        for (Map.Entry<PlanNode, CanonicalPlan> entry : context.getCanonicalPlans().entrySet()) {
            CanonicalPlan canonicalPlan = entry.getValue();
            PlanNode plan = entry.getKey();
            if (enableVerboseRuntimeStats) {
                profileStartTime = System.nanoTime();
            }

            String canonicalPlanString = canonicalPlan.toString(objectMapper);
            String hashValue = hashCanonicalPlan(canonicalPlanString);
            if (plan == key.getNode() && isRootNode && logQueryPlansUsedInHistoryBasedOptimizer(session)) {
                historyBasedStatisticsCacheManager.getCanonicalPlan(session.getQueryId()).put(key.getStrategy(), canonicalPlanString);
            }

            if (enableVerboseRuntimeStats) {
                profileTime("HashCanonicalPlan", profileStartTime, session);
            }
            if (loadValueTimeout(startTimeInNano, timeoutInMilliseconds)) {
                return Optional.empty();
            }
            // Compute input table statistics for the plan node. This is useful in history based optimizations,
            // where historical plan statistics are reused if input tables are similar in size across runs.
            ImmutableList.Builder<PlanNodeCanonicalInfo.InputTableCacheKey> inputTableCacheKeyBuilder = ImmutableList.builder();
            ImmutableList.Builder<PlanStatistics> inputTableStatisticsBuilder = ImmutableList.builder();
            if (enableVerboseRuntimeStats) {
                profileStartTime = System.nanoTime();
            }
            for (TableScanNode scanNode : context.getInputTables().get(plan)) {
                if (loadValueTimeout(startTimeInNano, timeoutInMilliseconds)) {
                    return Optional.empty();
                }
                PlanNodeCanonicalInfo.InputTableCacheKey inputTableCacheKey = getInputTableCacheKey(scanNode);
                if (getHistoryBasedOptimizerInputStatisticsCheckStrategy(session).equals(ALWAYS)) {
                    inputTableStatisticsBuilder.add(getPlanStatisticsForTable(session, inputTableCacheKey, enableVerboseRuntimeStats));
                }
                else {
                    inputTableCacheKeyBuilder.add(inputTableCacheKey);
                }
            }
            if (enableVerboseRuntimeStats) {
                profileTime("GetPlanStatisticsForTable", profileStartTime, session);
            }
            cache.put(new CacheKey(plan, key.getStrategy()), new PlanNodeCanonicalInfo(hashValue,
                    getHistoryBasedOptimizerInputStatisticsCheckStrategy(session).equals(ALWAYS) ? Optional.of(inputTableStatisticsBuilder.build()) : Optional.empty(),
                    getHistoryBasedOptimizerInputStatisticsCheckStrategy(session).equals(ALWAYS) ? Optional.empty() : Optional.of(inputTableCacheKeyBuilder.build())));
        }
        return Optional.ofNullable(cache.get(key));
    }

    private boolean loadValueTimeout(long startTimeInNano, long timeoutInMilliseconds)
    {
        if (timeoutInMilliseconds == 0) {
            return false;
        }
        return NANOSECONDS.toMillis(System.nanoTime() - startTimeInNano) > timeoutInMilliseconds;
    }

    private void profileTime(String name, long startProfileTime, Session session)
    {
        session.getRuntimeStats().addMetricValue(String.format("CachingPlanCanonicalInfoProvider:%s", name), NANO, System.nanoTime() - startProfileTime);
    }

    private PlanNodeCanonicalInfo.InputTableCacheKey getInputTableCacheKey(TableScanNode table)
    {
        return new PlanNodeCanonicalInfo.InputTableCacheKey(new TableHandle(
                table.getTable().getConnectorId(),
                table.getTable().getConnectorHandle(),
                table.getTable().getTransaction(),
                Optional.empty()), ImmutableList.copyOf(table.getAssignments().values()), new Constraint<>(table.getCurrentConstraint()));
    }

    private PlanStatistics getPlanStatisticsForTable(Session session, PlanNodeCanonicalInfo.InputTableCacheKey key, boolean profileRuntime)
    {
        Map<PlanNodeCanonicalInfo.InputTableCacheKey, PlanStatistics> cache = historyBasedStatisticsCacheManager.getInputTableStatistics(session.getQueryId());
        PlanStatistics planStatistics = cache.get(key);
        if (planStatistics != null) {
            return planStatistics;
        }
        long startProfileTime = 0;
        if (profileRuntime) {
            startProfileTime = System.nanoTime();
        }
        TableStatistics tableStatistics = metadata.getTableStatistics(session, key.getTableHandle(), key.getColumnHandles(), key.getConstraint());
        if (profileRuntime) {
            profileTime("ReadFromMetaData", startProfileTime, session);
        }
        planStatistics = new PlanStatistics(tableStatistics.getRowCount(), tableStatistics.getTotalSize(), 1, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());
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

    private String hashCanonicalPlan(String planString)
    {
        return sha256().hashString(planString, UTF_8).toString();
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
}
