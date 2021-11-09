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

package com.facebook.presto.hive.functions;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.functions.HiveFunctionRegistry.Type;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class HiveFunctionModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(HiveFunctionModule.class);

    private final ClassLoader classLoader;

    public HiveFunctionModule(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(HiveFunctionConfig.class);
        Type registryType = buildConfigObject(HiveFunctionConfig.class).getRegistryType();
        log.info("Using %s function registry", registryType);
        switch (registryType) {
            case SIMPLE:
                binder.bind(HiveFunctionRegistry.class).to(SimpleHiveFunctionRegistry.class).in(Scopes.SINGLETON);
                break;
            default:
                throw new IllegalArgumentException("Unsupported function registry type: " + registryType);
        }
        binder.bind(FunctionNamespaceManager.class).to(HiveFunctionNamespaceManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForHiveFunction
    public ClassLoader getClassLoader()
    {
        return classLoader;
    }
}
