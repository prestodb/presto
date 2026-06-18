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
package com.facebook.presto.kafka;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Module;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConfigurationAwareModules
{
    private ConfigurationAwareModules() {}

    public static Module combine(Module... modules)
    {
        checkArgument(modules.length > 0, "No modules given");
        if (modules.length == 1) {
            return modules[0];
        }
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                for (Module module : modules) {
                    requireNonNull(module, "module is null");
                    install(module);
                }
            }
        };
    }
}
