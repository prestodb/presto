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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class RedisProviderModule
        extends AbstractConfigurationAwareModule
{
    public RedisProviderModule() {}

    @Provides
    @Singleton
    public RedisClusterAsyncCommands<String, HistoricalPlanStatistics> provideAsyncCommands(HistoricalStatisticsSerde historicalStatisticsSerde,
            RedisProviderConfig redisProviderConfig, AbstractRedisClient redisClient)
    {
        return RedisClusterAsyncCommandsFactory.getRedisClusterAsyncCommands(redisProviderConfig, historicalStatisticsSerde, redisClient);
    }

    @Provides
    @Singleton
    public AbstractRedisClient provideAbstractRedisClient(RedisProviderConfig redisProviderConfig, RedisProviderConfig config)
    {
        if (redisProviderConfig.getClusterModeEnabled()) {
            return RedisClusterAsyncCommandsFactory.getRedisClusterClient(config);
        }
        return RedisClusterAsyncCommandsFactory.getRedisClient(config);
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(RedisProviderConfig.class);
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        binder.bind(MBeanServer.class).toInstance(platformMBeanServer);
        binder.bind(RedisProviderApiStats.class).in(Scopes.SINGLETON);
        binder.bind(HistoricalStatisticsSerde.class).toInstance(new HistoricalStatisticsSerde());
        binder.bind(RedisPlanStatisticsProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RedisProviderApiStats.class).as(generatedNameOf(RedisProviderApiStats.class));
    }
}
