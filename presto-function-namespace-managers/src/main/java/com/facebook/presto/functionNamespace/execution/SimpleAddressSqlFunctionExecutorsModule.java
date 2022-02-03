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
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.thrift.SimpleAddressThriftSqlFunctionExecutionModule;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;

import java.util.Map;

import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class SimpleAddressSqlFunctionExecutorsModule
        extends AbstractConfigurationAwareModule
{
    // The ideal case is to have a Map<FunctionImplementationType, SqlFunctionExecutor> to handle multiple executors.
    // However, It is hard to create this map in Guice given SqlFunctionExecutor modules now are dynamically plugged in.
    // One way to solve this is to let each module initialize its own SqlFunctionExecutor during binding time and return to the current module.
    // Then the current module can bind the map to the collected SqlFunctionExecutor instances.
    // However, to initialize all these SqlFunctionExecutors requires another bunch of injected dependencies.
    // So we simplified the case to only allow one singleton.
    // The downside is that if there is a use case with two UDF services reachable by one cluster;
    // one UDF services is with grpc protocol and the other is thrift;
    // we need to come back to solve the problem.
    private final SqlFunctionExecutionModule sqlFunctionExecutorModule;

    public SimpleAddressSqlFunctionExecutorsModule()
    {
        // use thrift implementation as the default
        this(new SimpleAddressThriftSqlFunctionExecutionModule());
    }

    public SimpleAddressSqlFunctionExecutorsModule(SqlFunctionExecutionModule sqlFunctionExecutorModule)
    {
        this.sqlFunctionExecutorModule = requireNonNull(sqlFunctionExecutorModule, "sqlFunctionExecutorModule is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        SqlInvokedFunctionNamespaceManagerConfig config = buildConfigObject(SqlInvokedFunctionNamespaceManagerConfig.class);
        ImmutableMap.Builder<Language, FunctionImplementationType> languageImplementationTypeMap = ImmutableMap.builder();
        ImmutableMap.Builder<String, FunctionImplementationType> supportedLanguages = ImmutableMap.builder();
        for (String languageName : config.getSupportedFunctionLanguages()) {
            Language language = new Language(languageName);
            FunctionImplementationType implementationType = buildConfigObject(SqlFunctionLanguageConfig.class, languageName).getFunctionImplementationType();
            languageImplementationTypeMap.put(language, implementationType);
            supportedLanguages.put(languageName, implementationType);
        }

        // for SqlFunctionExecutor
        sqlFunctionExecutorModule.setSupportedLanguages(supportedLanguages.build());
        install(sqlFunctionExecutorModule);

        // for SqlFunctionExecutors
        binder.bind(SqlFunctionExecutors.class).in(SINGLETON);
        binder.bind(new TypeLiteral<Map<Language, FunctionImplementationType>>() {}).toInstance(languageImplementationTypeMap.build());
    }
}
