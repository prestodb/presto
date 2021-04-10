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
package com.facebook.presto.spark.execution;

import com.facebook.presto.common.Page;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PrestoSparkBroadcastTableCacheManager
{
    // Currently we cache HT from a single stage. When a task from another stage is scheduled, the cache will be cleared
    private final Map<BroadcastTableCacheKey, List<List<Page>>> cache = new HashMap<>();
    private long cacheSizeInBytes;

    public synchronized void removeCachedTablesForStagesOtherThan(StageId stageId)
    {
        Iterator<Map.Entry<BroadcastTableCacheKey, List<List<Page>>>> iterator = cache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BroadcastTableCacheKey, List<List<Page>>> entry = iterator.next();
            if (!entry.getKey().getStageId().equals(stageId)) {
                cacheSizeInBytes -= entry.getValue().stream().mapToLong(pageList -> pageList.stream().mapToLong(Page::getRetainedSizeInBytes).sum()).sum();
                iterator.remove();
            }
        }
    }

    public synchronized List<List<Page>> getCachedBroadcastTable(StageId stageId, PlanNodeId planNodeId)
    {
        return cache.get(new BroadcastTableCacheKey(stageId, planNodeId));
    }

    public synchronized void cache(StageId stageId, PlanNodeId planNodeId, List<List<Page>> broadcastTable)
    {
        cache.put(new BroadcastTableCacheKey(stageId, planNodeId), broadcastTable);
        cacheSizeInBytes += broadcastTable.stream().mapToLong(pageList -> pageList.stream().mapToLong(Page::getRetainedSizeInBytes).sum()).sum();
    }

    public synchronized long getCacheSizeInBytes()
    {
        return cacheSizeInBytes;
    }

    private static class BroadcastTableCacheKey
    {
        private final StageId stageId;
        private final PlanNodeId planNodeId;

        public BroadcastTableCacheKey(StageId stageId, PlanNodeId planNodeId)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
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
            BroadcastTableCacheKey that = (BroadcastTableCacheKey) o;
            return Objects.equals(stageId, that.stageId) &&
                    Objects.equals(planNodeId, that.planNodeId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(stageId, planNodeId);
        }

        public StageId getStageId()
        {
            return stageId;
        }
    }
}
