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
package com.facebook.presto.spark.execution.property;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Native configuration module that allows the population of config properties without config
 * annotation checks.
 */
public class NativeExecutionConfigModule
        extends AbstractModule
{
    private final Map<String, String> systemConfigs;

    public NativeExecutionConfigModule(Map<String, String> systemConfigs)
    {
        this.systemConfigs = ImmutableMap.copyOf(
                requireNonNull(systemConfigs, "systemConfigs is null"));
    }

    @Override
    protected void configure()
    {
        bind(new TypeLiteral<Map<String, String>>() {})
                .annotatedWith(
                        Names.named(NativeExecutionSystemConfig.NATIVE_EXECUTION_SYSTEM_CONFIG))
                .toInstance(systemConfigs);

        bind(NativeExecutionSystemConfig.class).in(Scopes.SINGLETON);
    }
}
