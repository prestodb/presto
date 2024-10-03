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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PlanCheckerProviderManager
{
    private final SimplePlanFragmentSerde simplePlanFragmentSerde;
    private final Map<String, PlanCheckerProviderFactory> providerFactories = new HashMap<>();
    private List<PlanCheckerProvider> providers = new ArrayList<>();

    @Inject
    public PlanCheckerProviderManager(SimplePlanFragmentSerde simplePlanFragmentSerde)
    {
        this.simplePlanFragmentSerde = requireNonNull(simplePlanFragmentSerde, "planNodeSerde is null");
    }

    public void addPlanCheckerProviderFactory(PlanCheckerProviderFactory planCheckerProviderFactory)
    {
        requireNonNull(planCheckerProviderFactory, "planCheckerProviderFactory is null");
        if (providerFactories.putIfAbsent(planCheckerProviderFactory.getName(), planCheckerProviderFactory) != null) {
            throw new IllegalArgumentException(format("PlanCheckerProviderFactory '%s' is already registered", planCheckerProviderFactory.getName()));
        }
    }

    public void loadPlanCheckerProviders()
    {
        if (!providerFactories.isEmpty()) {
            providers = providerFactories.values().stream().map(pc -> pc.create(simplePlanFragmentSerde)).collect(Collectors.toList());
        }
        else {
            providers = ImmutableList.of(new EmptyPlanCheckerProvider());
        }
    }

    public void updatePlanCheckerProviders(PlanChecker planChecker)
    {
        for (PlanCheckerProvider provider : providers) {
            planChecker.update(provider);
        }
    }

    public void updatePlanCheckerProviders(PlanFragmenter planFragmenter)
    {
        for (PlanCheckerProvider provider : providers) {
            planFragmenter.updatePlanCheckers(provider);
        }
    }

    public List<PlanCheckerProvider> getPlanCheckerProviders()
    {
        return providers;
    }

    public static class EmptyPlanCheckerProvider
            implements PlanCheckerProvider
    {
    }
}
