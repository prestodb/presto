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
package com.facebook.presto.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import static io.airlift.json.JsonBinder.jsonBinder;

public class PrestoJsonModule
        implements Module
{
    private static final String JSON_MODULE_AFTERBURNER_ENABLED = "json.module.after-burner.enabled";

    private boolean includeAfterBurnerModule;

    @VisibleForTesting
    public PrestoJsonModule(boolean includeAfterBurnerModule)
    {
        this.includeAfterBurnerModule = includeAfterBurnerModule;
    }

    public PrestoJsonModule()
    {
        this.includeAfterBurnerModule = Boolean.parseBoolean(System.getProperty(JSON_MODULE_AFTERBURNER_ENABLED));
    }

    @Override
    public void configure(Binder binder)
    {
        binder.disableCircularProxies();
        binder.bind(ObjectMapper.class).toProvider(ObjectMapperProvider.class);
        if (includeAfterBurnerModule) {
            jsonBinder(binder).addModuleBinding().to(AfterburnerModule.class);
        }
        binder.bind(JsonCodecFactory.class).in(Scopes.SINGLETON);
    }
}
