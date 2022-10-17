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
package com.facebook.presto.cost;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.statistics.EmptyPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.ExternalPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.ExternalPlanStatisticsProviderFactory;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.CachingPlanCanonicalInfoProvider;
import com.facebook.presto.sql.planner.PlanCanonicalInfoProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsManager
{
    private final SessionPropertyManager sessionPropertyManager;
    private final PlanCanonicalInfoProvider planCanonicalInfoProvider;
    private final HistoryBasedOptimizationConfig config;

    private HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider = EmptyPlanStatisticsProvider.getInstance();
    private boolean statisticsProviderAdded;
    private static final Logger log = Logger.get(HistoryBasedPlanStatisticsManager.class);
    private static final File EXTERNAL_PLAN_STATISTICS_PROVIDER_CONFIG = new File("etc/external-plan-statistics-provider.properties");
    private static final String EXTERNAL_PLAN_STATISTICS_PROVIDER_PROPERTY_NAME = "external-plan-statistics-provider.factory";
    private static final String DEFAULT_EXTERNAL_PLAN_STATISTICS_PROVIDER_FACTORY_NAME = "smarter-warehouse";
    private ExternalPlanStatisticsProvider externalPlanStatisticsProvider;
    private final Map<String, ExternalPlanStatisticsProviderFactory> externalPlanStatisticsProviderFactories = new ConcurrentHashMap<>();

    private final Metadata metadata;
    private boolean externalProviderAdded;

    @Inject
    public HistoryBasedPlanStatisticsManager(ObjectMapper objectMapper, SessionPropertyManager sessionPropertyManager, Metadata metadata, HistoryBasedOptimizationConfig config)
    {
        requireNonNull(objectMapper, "objectMapper is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.planCanonicalInfoProvider = new CachingPlanCanonicalInfoProvider(objectMapper, metadata);
        this.config = requireNonNull(config, "config is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public void addHistoryBasedPlanStatisticsProviderFactory(HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider)
    {
        if (statisticsProviderAdded) {
            throw new IllegalStateException("historyBasedPlanStatisticsProvider can only be set once");
        }
        this.historyBasedPlanStatisticsProvider = historyBasedPlanStatisticsProvider;
        statisticsProviderAdded = true;
    }

    public void addExternalPlanStatisticsProviderFactory(ExternalPlanStatisticsProviderFactory factory)
    {
        requireNonNull(factory, "ExternalPlanStatisticsProviderFactory is null");

        if (externalPlanStatisticsProviderFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("ExternalPlanStatisticsProviderFactory '%s' is already registered", factory.getName()));
        }
    }

    public HistoryBasedPlanStatisticsCalculator getHistoryBasedPlanStatisticsCalculator(StatsCalculator delegate)
    {
        return new HistoryBasedPlanStatisticsCalculator(() -> historyBasedPlanStatisticsProvider, delegate,
                planCanonicalInfoProvider, config, () -> externalPlanStatisticsProvider, metadata);
    }

    public HistoryBasedPlanStatisticsTracker getHistoryBasedPlanStatisticsTracker()
    {
        return new HistoryBasedPlanStatisticsTracker(() -> historyBasedPlanStatisticsProvider, sessionPropertyManager, config);
    }

    public PlanCanonicalInfoProvider getPlanCanonicalInfoProvider()
    {
        return planCanonicalInfoProvider;
    }

    public void loadExternalPlanStatisticsProvider()
            throws Exception
    {
        if (EXTERNAL_PLAN_STATISTICS_PROVIDER_CONFIG.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(EXTERNAL_PLAN_STATISTICS_PROVIDER_CONFIG));
            String factoryName = properties.remove(EXTERNAL_PLAN_STATISTICS_PROVIDER_PROPERTY_NAME);

            checkArgument(!isNullOrEmpty(factoryName),
                    "External Plan Statistics Provider configuration %s does not contain %s", EXTERNAL_PLAN_STATISTICS_PROVIDER_CONFIG.getAbsoluteFile(),
                    EXTERNAL_PLAN_STATISTICS_PROVIDER_PROPERTY_NAME);
            load(factoryName, properties);
        }
        else {
            load(DEFAULT_EXTERNAL_PLAN_STATISTICS_PROVIDER_FACTORY_NAME, ImmutableMap.of());
        }
    }

    @VisibleForTesting
    public void load(String factoryName, Map<String, String> properties)
    {
        log.info("-- Loading External Plan Statistics Provider factory --");

        ExternalPlanStatisticsProviderFactory factory = externalPlanStatisticsProviderFactories.get(factoryName);
        checkState(factory != null, "External Plan Statistics Provider factory %s is not registered", factoryName);

        ExternalPlanStatisticsProvider provider = factory.create(properties);

        if (externalProviderAdded) {
            throw new IllegalStateException("ExternalPlanStatisticsProvider can only be set once");
        }

        this.externalPlanStatisticsProvider = provider;
        externalProviderAdded = true;

        log.info("-- Loaded External Plan Statistics Provider %s --", factoryName);
    }
}
