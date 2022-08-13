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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import io.airlift.units.DataSize;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CachingPlanHasher
        implements PlanHasher
{
    private static final DataSize CACHE_SIZE_BYTES = new DataSize(1, DataSize.Unit.MEGABYTE);

    // Create a cache, instead of a LoadingCache, because we can load multiple keys at once.
    // For weight, we only consider size of hash, as PlanNodes are already in memory for running queries.
    // We use length of hash + 20 bytes as overhead for storing references, string size and strategy.
    private final Cache<CacheKey, String> cache = CacheBuilder.newBuilder()
            .maximumWeight(CACHE_SIZE_BYTES.toBytes())
            .weigher((Weigher<CacheKey, String>) (key, hash) -> hash.length() + 20)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();
    private final ObjectMapper objectMapper;

    public CachingPlanHasher(ObjectMapper objectMapper)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @Override
    public Optional<String> hash(PlanNode planNode, PlanCanonicalizationStrategy strategy)
    {
        // TODO: Use QueryManager to unload keys rather relying on LoadingCache
        CacheKey key = new CacheKey(planNode, strategy);
        String result = cache.getIfPresent(key);
        if (result != null) {
            return Optional.of(result);
        }

        CanonicalPlanGenerator.Context context = new CanonicalPlanGenerator.Context();
        key.getNode().accept(new CanonicalPlanGenerator(key.getStrategy(), objectMapper), context);
        context.getCanonicalPlans().forEach((plan, canonicalPlan) -> {
            String hashValue = hashCanonicalPlan(canonicalPlan, objectMapper);
            cache.put(new CacheKey(plan, strategy), hashValue);
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
