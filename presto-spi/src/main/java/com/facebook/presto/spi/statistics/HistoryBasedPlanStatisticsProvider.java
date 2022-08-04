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

/**
 * An interface to provide plan statistics from an external source to Presto planner.
 * A simple implementation can be a key value store using provided canonical plan hashes of plan nodes.
 * More advanced implementations can parse the PlanNode, and predict statistics.
 */
public interface HistoryBasedPlanStatisticsProvider
{
    String getName();

    /**
     * Given a list of plan node hashes, returns historical statistics for them.
     * Some entries in return value may be missing if no corresponding history exists.
     * This can be called even when hash of a plan node is not present.
     *
     * TODO: Using PlanNode as map key can be expensive, we can use Plan node id as a map key.
     */
    Map<PlanNodeWithHash, HistoricalPlanStatistics> getStats(List<PlanNodeWithHash> planNodesWithHash);

    /**
     * Given plan hashes and corresponding statistics after a query is run, store them for future retrieval.
     */
    void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics);
}
