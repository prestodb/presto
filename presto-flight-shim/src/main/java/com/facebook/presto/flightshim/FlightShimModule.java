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
import com.google.inject.Scopes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class FlightShimModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(FlightShimPluginManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferAllocator.class).to(RootAllocator.class).in(Scopes.SINGLETON);
        binder.bind(FlightShimConfig.class).in(Scopes.SINGLETON);
        binder.bind(FlightShimProducer.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PluginManagerConfig.class);
        configBinder(binder).bindConfig(StaticCatalogStoreConfig.class);
    }
}
