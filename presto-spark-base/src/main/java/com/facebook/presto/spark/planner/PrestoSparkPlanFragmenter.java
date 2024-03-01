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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.SubPlan;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PrestoSparkPlanFragmenter
{
    private final PlanFragmenter planFragmenter;

    @Inject
    public PrestoSparkPlanFragmenter(PlanFragmenter planFragmenter)
    {
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
    }

    public SubPlan fragmentQueryPlan(Session session, Plan plan, WarningCollector warningCollector)
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator()
        {
            @Override
            public PlanNodeId getNextId()
            {
                throw new UnsupportedOperationException("materialized execution is not supported by the presto on spark");
            }
        };
        return planFragmenter.createSubPlans(session, plan, false, planNodeIdAllocator, warningCollector);
    }
}
