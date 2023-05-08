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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.encoding.StringUtils.UTF_8;
import static com.google.common.hash.Hashing.sha256;

public class CachingPlanAnalyticsInfoProvider
        implements PlanCanonicalInfoProvider
{
    private static final DataSize CACHE_SIZE_BYTES = new DataSize(2, DataSize.Unit.MEGABYTE);
    private final Cache<PlanNode, String> cache =
            CacheBuilder.newBuilder()
                    .maximumWeight(CACHE_SIZE_BYTES.toBytes())
                    .weigher((Weigher<PlanNode, String>) (key, value) -> value.length() + 20)
                    .expireAfterWrite(5, TimeUnit.MINUTES)
                    .build();

    private final ObjectMapper objectMapper;
    private final Metadata metadata;

    public CachingPlanAnalyticsInfoProvider(ObjectMapper objectMapper, Metadata metadata)
    {
        this.objectMapper = objectMapper;
        this.metadata = metadata;
    }

    @Override
    public Optional<String> hash(Session session, PlanNode planNode, PlanCanonicalizationStrategy strategy)
    {
        String result = cache.getIfPresent(planNode);
        if (result != null) {
            return Optional.of(result);
        }
        CanonicalPlanGenerator.Context context = new CanonicalPlanGenerator.Context();
        planNode.accept(
                new CanonicalPlanGenerator(PlanCanonicalizationStrategy.EXACT, objectMapper, session),
                context);
        context
                .getCanonicalPlans()
                .forEach(
                        (p, canonicalPlan) -> {
                            cache.put(p, hashCanonicalPlan(canonicalPlan, objectMapper));
                        });
        return Optional.ofNullable(cache.getIfPresent(planNode));
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
}
