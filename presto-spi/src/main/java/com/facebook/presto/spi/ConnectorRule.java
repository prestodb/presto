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
package com.facebook.presto.spi;

import com.facebook.presto.spi.plan.PlanNode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface ConnectorRule
{
    /**
     * Match a specific PlanNode
     */
    PlanNode match();

    /**
     * Apply the transformation on the matched node
     */
    Result apply(PlanNode planNode);

    public final class Result
    {
        public static Result empty()
        {
            return new Result(Optional.empty());
        }

        public static Result ofPlanNode(PlanNode transformedPlan)
        {
            return new Result(Optional.of(transformedPlan));
        }

        private final Optional<PlanNode> transformedPlan;

        private Result(Optional<PlanNode> transformedPlan)
        {
            this.transformedPlan = requireNonNull(transformedPlan, "transformedPlan is null");
        }

        public PlanNode getTransformedPlan()
        {
            return transformedPlan.get();
        }

        public boolean isEmpty()
        {
            return !transformedPlan.isPresent();
        }
    }
}
