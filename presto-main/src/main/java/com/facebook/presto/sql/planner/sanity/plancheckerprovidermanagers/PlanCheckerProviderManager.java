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
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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
        this.provider = new EmptyPlanCheckerProvider();
    }

    public void addPlanCheckerProviderFactory(PlanCheckerProviderFactory providerFactory)
    {
        if (this.providerFactory != null) {
            throw new IllegalArgumentException("PlanCheckerProviderFactory is already added, only one factory supported");
        }
        this.providerFactory = requireNonNull(providerFactory, "providerFactory is null");
    }

    public PlanCheckerProvider getPlanCheckerProvider()
    {
        return provider;
    }

    public void loadPlanCheckerProvider()
            throws IOException
    {
        if (providerFactory != null) {
            provider = providerFactory.create(getConfig(), simplePlanFragmentSerde);
        }
    }

    private Map<String, String> getConfig()
            throws IOException
    {
        Map<String, String> properties;
        if (PLAN_CHECKER_PROVIDER_CONFIG.exists()) {
            properties = Collections.unmodifiableMap(new HashMap<>(loadProperties(PLAN_CHECKER_PROVIDER_CONFIG)));
        }
        else {
            properties = Collections.emptyMap();
        }
        return properties;
    }

    public static class EmptyPlanCheckerProvider
            implements PlanCheckerProvider
    {
    }
}
