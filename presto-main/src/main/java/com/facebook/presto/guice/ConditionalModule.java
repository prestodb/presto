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
package com.facebook.presto.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ConditionalModule
        implements ConfigurationAwareModule
{
    public static ConfigurationAwareModule installIfPropertyEquals(Module module, String property, String expectedValue)
    {
        return new ConditionalModule(module, property, expectedValue);
    }

    private final Module module;
    private final String property;
    private final String expectedValue;
    private ConfigurationFactory configurationFactory;

    private ConditionalModule(Module module, String property, String expectedValue)
    {
        this.module = checkNotNull(module, "module is null");
        this.property = checkNotNull(property, "property is null");
        this.expectedValue = checkNotNull(expectedValue, "expectedValue is null");
    }

    @Override
    public void setConfigurationFactory(ConfigurationFactory configurationFactory)
    {
        this.configurationFactory = checkNotNull(configurationFactory, "configurationFactory is null");
        configurationFactory.consumeProperty(property);

        // consume properties if we are not going to install the module
        if (!shouldInstall()) {
            configurationFactory.registerConfigurationClasses(module);
        }
    }

    @Override
    public void configure(Binder binder)
    {
        checkState(configurationFactory != null, "configurationFactory was not set");
        if (!configurationFactory.getProperties().containsKey(property)) {
            binder.addError("Required configuration property '%s' was not set", property);
        }
        if (shouldInstall()) {
            binder.install(module);
        }
    }

    private boolean shouldInstall()
    {
        return expectedValue.equalsIgnoreCase(configurationFactory.getProperties().get(property));
    }
}
