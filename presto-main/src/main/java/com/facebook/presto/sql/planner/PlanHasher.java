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

import java.util.Optional;

/**
 * Interface to hash a PlanNode according to PlanCanonicalizationStrategy `strategy`.
 */
public interface PlanHasher
{
    /**
     * Canonicalize the plan using `CanonicalPlanGenerator` with given strategy, and returns a hash representation
     * of the canonicalized plan.
     * @param planNode Plan node to hash
     * @param strategy Strategy to canonicalize the plan node
     * @return Hash of the plan node. Returns Optional.empty() if unable to hash.
     */
    Optional<String> hash(PlanNode planNode, PlanCanonicalizationStrategy strategy);
}
