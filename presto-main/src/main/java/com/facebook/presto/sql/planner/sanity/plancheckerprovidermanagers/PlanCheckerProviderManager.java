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
package com.facebook.presto.sql.planner.sanity.plancheckerprovidermanagers;

import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static java.util.Objects.requireNonNull;

public class PlanCheckerProviderManager
{
    private static final File PLAN_CHECKER_PROVIDER_CONFIG = new File("etc/plan-checker-provider.properties");
    private final SimplePlanFragmentSerde simplePlanFragmentSerde;
    private PlanCheckerProviderFactory providerFactory;
    private PlanCheckerProvider provider;

    @Inject
    public PlanCheckerProviderManager(SimplePlanFragmentSerde simplePlanFragmentSerde)
    {
        this.simplePlanFragmentSerde = requireNonNull(simplePlanFragmentSerde, "planNodeSerde is null");
    }

    public void addPlanCheckerProviderFactory(PlanCheckerProviderFactory providerFactory)
    {
        if (this.providerFactory != null) {
            throw new IllegalArgumentException("PlanCheckerProviderFactory is already added, only one factory supported");
        }
        this.providerFactory = requireNonNull(providerFactory, "providerFactory is null");
    }

    public void loadPlanCheckerProvider(PlanChecker planChecker, PlanFragmenter planFragmenter)
            throws IOException
    {
        loadPlanCheckerProvider();
        planChecker.update(provider);
        planFragmenter.updatePlanCheckers(provider);
    }

    public void loadPlanCheckerProvider()
            throws IOException
    {
        if (providerFactory != null) {
            provider = providerFactory.create(getConfig(), simplePlanFragmentSerde);
        }
        else {
            provider = new EmptyPlanCheckerProvider();
        }
    }

    public PlanCheckerProvider getPlanCheckerProvider()
    {
        if (provider == null) {
            try {
                loadPlanCheckerProvider();
            }
            catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        return provider;
    }

    private Map<String, String> getConfig()
            throws IOException
    {
        if (PLAN_CHECKER_PROVIDER_CONFIG.exists()) {
            return loadProperties(PLAN_CHECKER_PROVIDER_CONFIG);
        }
        return ImmutableMap.of();
    }

    public static class EmptyPlanCheckerProvider
            implements PlanCheckerProvider
    {
    }
}
