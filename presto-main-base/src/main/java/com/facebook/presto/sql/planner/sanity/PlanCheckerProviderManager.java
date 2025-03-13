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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.PlanCheckerProviderContext;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PlanCheckerProviderManager
{
    private static final Logger log = Logger.get(PlanCheckerProviderManager.class);
    private static final String PLAN_CHECKER_PROVIDER_NAME = "plan-checker-provider.name";

    private final SimplePlanFragmentSerde simplePlanFragmentSerde;
    private final Map<String, PlanCheckerProviderFactory> providerFactories = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<PlanCheckerProvider> providers = new CopyOnWriteArrayList<>();
    private final File configDirectory;

    @Inject
    public PlanCheckerProviderManager(SimplePlanFragmentSerde simplePlanFragmentSerde, PlanCheckerProviderManagerConfig config)
    {
        this.simplePlanFragmentSerde = requireNonNull(simplePlanFragmentSerde, "planNodeSerde is null");
        requireNonNull(config, "config is null");
        this.configDirectory = requireNonNull(config.getPlanCheckerConfigurationDir(), "configDirectory is null");
    }

    public void addPlanCheckerProviderFactory(PlanCheckerProviderFactory planCheckerProviderFactory)
    {
        requireNonNull(planCheckerProviderFactory, "planCheckerProviderFactory is null");
        if (providerFactories.putIfAbsent(planCheckerProviderFactory.getName(), planCheckerProviderFactory) != null) {
            throw new IllegalArgumentException(format("PlanCheckerProviderFactory '%s' is already registered", planCheckerProviderFactory.getName()));
        }
    }

    public void loadPlanCheckerProviders(NodeManager nodeManager)
            throws IOException
    {
        PlanCheckerProviderContext planCheckerProviderContext = new PlanCheckerProviderContext(simplePlanFragmentSerde, nodeManager);

        for (File file : listFiles(configDirectory)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                // unlike function namespaces and connectors, we don't have a concept of catalog
                // name here (conventionally config file name without the extension)
                // because plan checkers are never referenced by name.
                Map<String, String> properties = new HashMap<>(loadProperties(file));
                checkState(!isNullOrEmpty(properties.get(PLAN_CHECKER_PROVIDER_NAME)),
                        "Plan checker configuration %s does not contain %s",
                        file.getAbsoluteFile(),
                        PLAN_CHECKER_PROVIDER_NAME);
                String planCheckerProviderName = properties.remove(PLAN_CHECKER_PROVIDER_NAME);
                log.info("-- Loading plan checker provider [%s] --", planCheckerProviderName);
                PlanCheckerProviderFactory providerFactory = providerFactories.get(planCheckerProviderName);
                checkState(providerFactory != null,
                        "No planCheckerProviderFactory found for '%s'. Available factories were %s", planCheckerProviderName, providerFactories.keySet());
                providers.addIfAbsent(providerFactory.create(properties, planCheckerProviderContext));
                log.info("-- Added plan checker provider [%s] --", planCheckerProviderName);
            }
        }
    }

    private static List<File> listFiles(File dir)
    {
        if (dir != null && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    public List<PlanCheckerProvider> getPlanCheckerProviders()
    {
        return providers;
    }
}
