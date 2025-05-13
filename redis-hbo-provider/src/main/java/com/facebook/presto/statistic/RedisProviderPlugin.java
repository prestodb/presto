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
package com.facebook.presto.statistic;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.statistics.EmptyPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.lettuce.core.AbstractRedisClient;

import java.util.Map;

import static com.facebook.presto.statistic.PropertiesUtil.initializeConfigs;

public class RedisProviderPlugin
        implements Plugin, AutoCloseable
{
    private static final Logger log = Logger.get(RedisProviderPlugin.class);
    private final Map<String, String> propertyMap;

    private Injector injector;

    public RedisProviderPlugin(String configPath)
    {
        // This plugin cannot be loaded like other plugins because this is not a connector
        this(initializeConfigs(configPath));
    }

    public RedisProviderPlugin()
    {
        this(RedisProviderConfig.REDIS_PROPERTIES_PATH);
    }

    @VisibleForTesting
    public RedisProviderPlugin(Map<String, String> configs)
    {
        this.propertyMap = ImmutableMap.copyOf(configs);
    }

    public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
    {
        if (this.propertyMap == null) {
            log.info("Redis Provider Plugin returned an empty instance");
            return ImmutableList.of(EmptyPlanStatisticsProvider.getInstance());
        }
        if (!(propertyMap.getOrDefault(RedisProviderConfig.COORDINATOR_PROPERTY_NAME, "false").equals("true") &&
                propertyMap.getOrDefault(RedisProviderConfig.HBO_PROVIDER_ENABLED_NAME, "false").equals("true"))) {
            return ImmutableList.of(EmptyPlanStatisticsProvider.getInstance());
        }
        injector = RedisProviderInjectorFactory.create(propertyMap);
        log.info("Redis Provider Plugin could successfully create the Redis Provider");
        return ImmutableList.of(injector.getInstance(RedisPlanStatisticsProvider.class));
    }

    @VisibleForTesting
    public void close()
    {
        if (injector != null) {
            injector.getInstance(AbstractRedisClient.class).close();
        }
    }
}
