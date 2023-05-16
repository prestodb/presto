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
package com.facebook.presto.server;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.resourcemanager.ForPeriodicTaskExecutor;
import com.facebook.presto.resourcemanager.ResourceManagerConfig;
import com.facebook.presto.util.PeriodicTaskExecutorFactory;
import com.facebook.presto.util.SimplePeriodicTaskExecutorFactory;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.ConcurrentScheduledExecutor.createConcurrentScheduledExecutor;
import static com.google.common.base.Preconditions.checkArgument;

public class PeriodicTaskExecutorModule
        extends AbstractConfigurationAwareModule
{
    private ResourceManagerConfig config;

    @Override
    protected void setup(Binder binder)
    {
        config = buildConfigObject(ResourceManagerConfig.class);
        binder.bind(PeriodicTaskExecutorFactory.class).to(SimplePeriodicTaskExecutorFactory.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForPeriodicTaskExecutor
    public Optional<ScheduledExecutorService> scheduledExecutorService()
    {
        return Optional.of(createConcurrentScheduledExecutor("resource-manager-heartbeats", config.getHeartbeatConcurrency(), config.getHeartbeatThreads()));
    }

    @Provides
    @Singleton
    @ForPeriodicTaskExecutor
    public ExecutorService executorService(@ForPeriodicTaskExecutor Optional<ScheduledExecutorService> scheduledExecutorService)
    {
        checkArgument(scheduledExecutorService.isPresent());
        return scheduledExecutorService.get();
    }
}
