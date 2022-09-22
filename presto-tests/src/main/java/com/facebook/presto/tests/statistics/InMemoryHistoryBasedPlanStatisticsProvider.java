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
package com.facebook.presto.tests.statistics;

import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.testng.Assert.assertTrue;

public class InMemoryHistoryBasedPlanStatisticsProvider
        implements HistoryBasedPlanStatisticsProvider
{
    private final Map<String, HistoricalPlanStatistics> cache = new ConcurrentHashMap<>();
    // Since, event processing happens in a different thread, we use a semaphore to wait for
    // all query events to process and finish.
    private final Semaphore semaphore = new Semaphore(1);

    public InMemoryHistoryBasedPlanStatisticsProvider()
    {
        semaphore.acquireUninterruptibly();
    }

    @Override
    public String getName()
    {
        return "test";
    }

    @Override
    public Map<PlanNodeWithHash, HistoricalPlanStatistics> getStats(List<PlanNodeWithHash> planNodeHashes)
    {
        return planNodeHashes.stream().collect(toImmutableMap(
                planNodeWithHash -> planNodeWithHash,
                planNodeWithHash -> {
                    if (planNodeWithHash.getHash().isPresent()) {
                        return cache.getOrDefault(planNodeWithHash.getHash().get(), HistoricalPlanStatistics.empty());
                    }
                    return HistoricalPlanStatistics.empty();
                }));
    }

    @Override
    public void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics)
    {
        hashesAndStatistics.forEach((planNodeWithHash, historicalPlanStatistics) -> {
            if (planNodeWithHash.getHash().isPresent()) {
                cache.put(planNodeWithHash.getHash().get(), historicalPlanStatistics);
            }
        });
        semaphore.release();
    }

    public void waitProcessQueryEvents()
    {
        try {
            assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));
        }
        catch (InterruptedException e) {
            throw new AssertionError("Query events could not be processed in time");
        }
    }

    @VisibleForTesting
    public void clearCache()
    {
        cache.clear();
    }
}
