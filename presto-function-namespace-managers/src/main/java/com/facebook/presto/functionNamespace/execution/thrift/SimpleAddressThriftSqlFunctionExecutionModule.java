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
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutionModule;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.facebook.presto.spi.function.SqlFunctionExecutor;
import com.facebook.presto.thrift.api.udf.ThriftUdfService;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;

import java.util.Map;

import static com.facebook.drift.client.guice.DriftClientBinder.driftClientBinder;
import static com.facebook.presto.functionNamespace.execution.thrift.ContextualSimpleAddressSelectorBinder.contextualSimpleAddressSelector;
import static com.facebook.presto.spi.function.FunctionImplementationType.THRIFT;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class SimpleAddressThriftSqlFunctionExecutionModule
        extends SqlFunctionExecutionModule
{
    @Override
    protected void setup(Binder binder)
    {
        ImmutableMap.Builder<Language, SimpleAddressSelectorConfig> thriftConfigBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Language, ThriftSqlFunctionExecutionConfig> thriftExecutionConfigs = ImmutableMap.builder();
        for (Map.Entry<String, FunctionImplementationType> entry : supportedLanguages.entrySet()) {
            String languageName = entry.getKey();
            Language language = new Language(languageName);
            FunctionImplementationType implementationType = entry.getValue();
            if (implementationType.equals(THRIFT)) {
                thriftConfigBuilder.put(language, buildConfigObject(SimpleAddressSelectorConfig.class, languageName));
                thriftExecutionConfigs.put(language, buildConfigObject(ThriftSqlFunctionExecutionConfig.class, languageName));
            }
        }

        Map<Language, SimpleAddressSelectorConfig> thriftConfigs = thriftConfigBuilder.build();
        if (thriftConfigs.isEmpty()) {
            binder.bind(SqlFunctionExecutor.class).to(NoopSqlFunctionExecutor.class).in(Scopes.SINGLETON);
            return;
        }
        binder.bind(SqlFunctionExecutor.class).to(ThriftSqlFunctionExecutor.class).in(Scopes.SINGLETON);

        driftClientBinder(binder)
                .bindDriftClient(ThriftUdfService.class)
                .withAddressSelector(
                        contextualSimpleAddressSelector(
                                thriftConfigs.entrySet().stream()
                                        .collect(toImmutableMap(entry -> entry.getKey().getLanguage(), Map.Entry::getValue))));
        binder.bind(new TypeLiteral<Map<Language, ThriftSqlFunctionExecutionConfig>>() {}).toInstance(thriftExecutionConfigs.build());
    }
}
