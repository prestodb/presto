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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.statistics.PlanStatistics;

import java.util.List;
import java.util.Optional;

/**
 * Interface to provide canonical info for a PlanNode.
 */
public interface PlanCanonicalInfoProvider
{
    /**
     * Canonicalize the plan using `CanonicalPlanGenerator` with given strategy, and returns a hash representation
     * of the canonicalized plan.
     * @param session Session for query being run
     * @param planNode Plan node to hash
     * @param strategy Strategy to canonicalize the plan node
     * @return Hash of the plan node. Returns Optional.empty() if unable to hash.
     */
    Optional<String> hash(Session session, PlanNode planNode, PlanCanonicalizationStrategy strategy);

    /**
     * Canonicalize the plan, and return statistics of input tables. Output order is consistent with
     * plan canonicalization.
     * @param session Session for query being run
     * @param planNode Plan node to hash
     * @return Statistics of leaf input tables to plan node, ordered by a consistent canonicalization strategy.
     */
    Optional<List<PlanStatistics>> getInputTableStatistics(Session session, PlanNode planNode);
}
