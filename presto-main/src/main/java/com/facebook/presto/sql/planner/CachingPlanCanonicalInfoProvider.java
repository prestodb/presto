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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.CONNECTOR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CachingPlanCanonicalInfoProvider
        implements PlanCanonicalInfoProvider
{
    private static final DataSize CACHE_SIZE_BYTES = new DataSize(10, DataSize.Unit.MEGABYTE);

    // Create a cache, instead of a LoadingCache, because we can load multiple keys at once.
    // For weight, we only consider size of hash, as PlanNodes are already in memory for running queries.
    // We use length of hash + 20 bytes as overhead for storing references, string size and strategy.
    private final Cache<CacheKey, PlanNodeCanonicalInfo> cache = CacheBuilder.newBuilder()
            .maximumWeight(CACHE_SIZE_BYTES.toBytes())
            .weigher((Weigher<CacheKey, PlanNodeCanonicalInfo>) (key, value) -> value.getHash().length() + 20)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();
    private final ObjectMapper objectMapper;
    private final Metadata metadata;

    public CachingPlanCanonicalInfoProvider(ObjectMapper objectMapper, Metadata metadata)
    {
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
        CacheKey key = new CacheKey(planNode, CONNECTOR);
        return loadValue(session, key).map(PlanNodeCanonicalInfo::getInputTableStatistics);
    }

    private Optional<PlanNodeCanonicalInfo> loadValue(Session session, CacheKey key)
    {
        // TODO: Use QueryManager to unload keys rather relying on LoadingCache
        PlanNodeCanonicalInfo result = cache.getIfPresent(key);
        if (result != null) {
            return Optional.of(result);
        }
        CanonicalPlanGenerator.Context context = new CanonicalPlanGenerator.Context();
        key.getNode().accept(new CanonicalPlanGenerator(key.getStrategy(), objectMapper), context);
        context.getCanonicalPlans().forEach((plan, canonicalPlan) -> {
            String hashValue = hashCanonicalPlan(canonicalPlan, objectMapper);
            // Compute input table statistics for the plan node. This is useful in history based optimizations,
            // where historical plan statistics are reused if input tables are similar in size across runs.
            List<PlanStatistics> inputTableStatistics = context.getInputTables().get(plan).stream()
                    .map(table -> metadata.getTableStatistics(
                            session,
                            // Remove table layout, so we don't include filter stats
                            new TableHandle(
                                    table.getTable().getConnectorId(),
                                    table.getTable().getConnectorHandle(),
                                    table.getTable().getTransaction(),
                                    Optional.empty()),
                            ImmutableList.copyOf(table.getAssignments().values()),
                            new Constraint<>(table.getCurrentConstraint())))
                    .map(tableStatistics -> new PlanStatistics(tableStatistics.getRowCount(), tableStatistics.getTotalSize(), 1))
                    .collect(toImmutableList());
            cache.put(new CacheKey(plan, key.getStrategy()), new PlanNodeCanonicalInfo(hashValue, inputTableStatistics));
        });
        return Optional.ofNullable(cache.getIfPresent(key));
    }

    @VisibleForTesting
    public long getCacheSize()
    {
        return cache.size();
    }

    private String hashCanonicalPlan(CanonicalPlan plan, ObjectMapper objectMapper)
    {
        return sha256().hashString(plan.toString(objectMapper), UTF_8).toString();
    }

    private static class CacheKey
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
