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
package com.facebook.presto.functionNamespace.execution.thrift;

import com.facebook.drift.client.address.SimpleAddressSelectorConfig;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.facebook.presto.thrift.api.udf.ThriftUdfService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.util.Providers;

import java.util.Map;

import static com.facebook.drift.client.guice.DriftClientBinder.driftClientBinder;
import static com.facebook.presto.functionNamespace.execution.thrift.ContextualSimpleAddressSelectorBinder.contextualSimpleAddressSelector;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class SimpleAddressThriftSqlFunctionExecutionModule
        implements Module
{
    private final Map<Language, SimpleAddressSelectorConfig> supportedLanguages;

    public SimpleAddressThriftSqlFunctionExecutionModule(Map<Language, SimpleAddressSelectorConfig> supportedLanguages)
    {
        this.supportedLanguages = requireNonNull(supportedLanguages, "supportedLanguages is null");
    }

    @Override
    public void configure(Binder binder)
    {
        if (supportedLanguages.isEmpty()) {
            binder.bind(ThriftSqlFunctionExecutor.class).toProvider(Providers.of(null));
            return;
        }
        binder.bind(ThriftSqlFunctionExecutor.class).in(SINGLETON);

        driftClientBinder(binder)
                .bindDriftClient(ThriftUdfService.class)
                .withAddressSelector(
                        contextualSimpleAddressSelector(
                                supportedLanguages.entrySet().stream()
                                        .collect(toImmutableMap(entry -> entry.getKey().getLanguage(), Map.Entry::getValue))));
    }
}
