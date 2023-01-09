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
package com.facebook.presto.spi.statistics;

import com.facebook.presto.spi.plan.PlanNodeWithHash;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class EmptyPlanStatisticsProvider
        implements HistoryBasedPlanStatisticsProvider
{
    private static final EmptyPlanStatisticsProvider SINGLETON = new EmptyPlanStatisticsProvider();

    @Override
    public String getName()
    {
        return "default";
    }

    @Override
    public Map<PlanNodeWithHash, HistoricalPlanStatistics> getStats(List<PlanNodeWithHash> planNodeHashes)
    {
        return emptyMap();
    }

    @Override
    public void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics)
    {
        // no op
    }

    public static EmptyPlanStatisticsProvider getInstance()
    {
        return SINGLETON;
    }
}
