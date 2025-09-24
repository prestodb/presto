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
package com.facebook.presto.flightshim;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.server.PluginManagerConfig;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.inject.Singleton;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class FlightShimModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(FlightShimPluginManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferAllocator.class).to(RootAllocator.class).in(Scopes.SINGLETON);
        binder.bind(FlightShimProducer.class).in(Scopes.SINGLETON);

        binder.bind(FlightShimServerExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FlightShimServerExecutionMBean.class).withGeneratedName();

        configBinder(binder).bindConfig(FlightShimConfig.class, FlightShimConfig.CONFIG_PREFIX);
        configBinder(binder).bindConfig(PluginManagerConfig.class);
        configBinder(binder).bindConfig(StaticCatalogStoreConfig.class);
    }

    @Provides
    @Singleton
    @ForFlightShimServer
    public static ExecutorService createFlightShimServerExecutor(FlightShimConfig config)
    {
        return new ThreadPoolExecutor(0, config.getReadSplitThreadPoolSize(), 1L, TimeUnit.MINUTES, new LinkedBlockingQueue(), threadsNamed("flight-shim-%s"));
    }
}
