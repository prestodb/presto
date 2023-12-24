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

import com.facebook.presto.Session;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.TypeProvider;

public interface PlanOptimizer
{
    PlanOptimizerResult optimize(PlanNode plan,
            Session session,
            TypeProvider types,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector);

    default boolean isEnabled(Session session)
    {
        return true;
    }

    default boolean isCostBased(Session session)
    {
        return false;
    }

    default String getStatsSource()
    {
        // source of statistics used for this optimizer: reimplement accordingly for each cost-based optimizer
        return null;
    }

    default void setEnabledForTesting(boolean isSet)
    {
        return;
    }

    default boolean isApplicable(PlanNode plan,
            Session session,
            TypeProvider types,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        setEnabledForTesting(true);

        boolean isApplicable = false;
        try {
            // wrap in try/catch block in case optimization throws an error
            PlanOptimizerResult optimizerResult = optimize(plan, session, types, variableAllocator, idAllocator, warningCollector);
            isApplicable = optimizerResult.isOptimizerTriggered();
        }
        finally {
            setEnabledForTesting(false);
            return isApplicable;
        }
    }
}
