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
package com.facebook.presto.functionNamespace.execution.rest;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutionModule;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;

import java.util.Map;

import static com.facebook.presto.spi.function.FunctionImplementationType.REST;
import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class SimpleAddressRestSqlFunctionExecutorsModule
        extends AbstractConfigurationAwareModule
{
    private final SqlFunctionExecutionModule sqlFunctionExecutorModule;

    public SimpleAddressRestSqlFunctionExecutorsModule()
    {
        this(new RestSqlFunctionExecutionModule());
    }

    public SimpleAddressRestSqlFunctionExecutorsModule(SqlFunctionExecutionModule sqlFunctionExecutorModule)
    {
        this.sqlFunctionExecutorModule = requireNonNull(sqlFunctionExecutorModule, "sqlFunctionExecutorModule is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        Map<Language, FunctionImplementationType> languageImplementationTypeMap = ImmutableMap.of(new Language("CPP"), REST);
        Map<String, FunctionImplementationType> supportedLanguages = ImmutableMap.of("CPP", REST);
        // for SqlFunctionExecutor
        sqlFunctionExecutorModule.setSupportedLanguages(supportedLanguages);
        install(sqlFunctionExecutorModule);
        // for SqlFunctionExecutors
        binder.bind(SqlFunctionExecutors.class).in(SINGLETON);
        binder.bind(new TypeLiteral<Map<Language, FunctionImplementationType>>() {}).toInstance(languageImplementationTypeMap);
    }
}
