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
package com.facebook.presto.hive.gcs;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class HiveGcsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(HiveGcsConfig.class);
        binder.bind(GcsConfigurationInitializer.class).to(HiveGcsConfigurationInitializer.class).in(Scopes.SINGLETON);

        if (buildConfigObject(HiveGcsConfig.class).isUseGcsAccessToken()) {
            newSetBinder(binder, DynamicConfigurationProvider.class).addBinding().to(GcsConfigurationProvider.class).in(Scopes.SINGLETON);
        }
    }
}
