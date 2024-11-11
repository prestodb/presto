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
package com.facebook.presto.execution;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.plan.PlanChecker;
import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.PlanCheckerProviderContext;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestingPlanCheckerProviderPlugin
        implements Plugin, PlanCheckerProviderFactory, PlanCheckerProvider
{
    private final AtomicBoolean triggerValidationFailure;

    public TestingPlanCheckerProviderPlugin(AtomicBoolean triggerValidationFailure)
    {
        this.triggerValidationFailure = triggerValidationFailure;
    }

    @Override
    public Iterable<PlanCheckerProviderFactory> getPlanCheckerProviderFactories()
    {
        return ImmutableList.of(this);
    }

    @Override
    public String getName()
    {
        return "TestPlanCheckers";
    }

    @Override
    public PlanCheckerProvider create(Map<String, String> properties, PlanCheckerProviderContext planCheckerProviderContext)
    {
        return this;
    }

    @Override
    public List<PlanChecker> getIntermediatePlanCheckers()
    {
        return ImmutableList.of(new TriggerFailurePlanChecker(triggerValidationFailure));
    }
}
