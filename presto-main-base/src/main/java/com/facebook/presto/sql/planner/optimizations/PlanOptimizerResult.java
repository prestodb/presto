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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public final class PlanOptimizerResult
{
    private final PlanNode planNode;
    private final boolean optimizerTriggered;

    public static PlanOptimizerResult optimizerResult(PlanNode planNode, boolean optimizerTriggered)
    {
        return new PlanOptimizerResult(planNode, optimizerTriggered);
    }
    private PlanOptimizerResult(PlanNode planNode, boolean optimizerTriggered)
    {
        this.planNode = requireNonNull(planNode, "planNode is null");
        this.optimizerTriggered = optimizerTriggered;
    }

    public PlanNode getPlanNode()
    {
        return planNode;
    }

    public boolean isOptimizerTriggered()
    {
        return optimizerTriggered;
    }
}
