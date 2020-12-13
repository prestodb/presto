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
package com.facebook.presto.functionNamespace.execution;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.drift.client.address.SimpleAddressSelectorConfig;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.thrift.SimpleAddressThriftSqlFunctionExecutionModule;
import com.facebook.presto.functionNamespace.execution.thrift.ThriftSqlFunctionExecutionConfig;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;

import java.util.Map;

import static com.facebook.presto.spi.function.FunctionImplementationType.THRIFT;
import static com.google.inject.Scopes.SINGLETON;

public class SimpleAddressSqlFunctionExecutorsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(SqlFunctionExecutors.class).in(SINGLETON);

        SqlInvokedFunctionNamespaceManagerConfig config = buildConfigObject(SqlInvokedFunctionNamespaceManagerConfig.class);
        ImmutableMap.Builder<Language, SimpleAddressSelectorConfig> thriftConfigs = ImmutableMap.builder();
        ImmutableMap.Builder<Language, FunctionImplementationType> languageImplementationTypeMap = ImmutableMap.builder();
        ImmutableMap.Builder<Language, ThriftSqlFunctionExecutionConfig> thriftExecutionConfigs = ImmutableMap.builder();
        for (String languageName : config.getSupportedFunctionLanguages()) {
            Language language = new Language(languageName);
            FunctionImplementationType implementationType = buildConfigObject(SqlFunctionLanguageConfig.class, languageName).getFunctionImplementationType();
            languageImplementationTypeMap.put(language, implementationType);
            if (implementationType.equals(THRIFT)) {
                thriftConfigs.put(language, buildConfigObject(SimpleAddressSelectorConfig.class, languageName));
                thriftExecutionConfigs.put(language, buildConfigObject(ThriftSqlFunctionExecutionConfig.class, languageName));
            }
        }
        binder.bind(new TypeLiteral<Map<Language, FunctionImplementationType>>() {}).toInstance(languageImplementationTypeMap.build());
        binder.install(new SimpleAddressThriftSqlFunctionExecutionModule(thriftConfigs.build()));
        binder.bind(new TypeLiteral<Map<Language, ThriftSqlFunctionExecutionConfig>>() {}).toInstance(thriftExecutionConfigs.build());
    }
}
