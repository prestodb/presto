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

import com.facebook.presto.functionNamespace.execution.thrift.ThriftSqlFunctionExecutor;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.google.inject.Inject;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SqlFunctionExecutors
{
    private final Map<Language, FunctionImplementationType> supportedLanguages;
    private final ThriftSqlFunctionExecutor thriftSqlFunctionExecutor;

    @Inject
    public SqlFunctionExecutors(Map<Language, FunctionImplementationType> supportedLanguages, ThriftSqlFunctionExecutor thriftSqlFunctionExecutor)
    {
        this.supportedLanguages = requireNonNull(supportedLanguages, "supportedLanguages is null");
        this.thriftSqlFunctionExecutor = requireNonNull(thriftSqlFunctionExecutor, "thriftSqlFunctionExecutor is null");
    }

    public Set<Language> getSupportedLanguages()
    {
        return supportedLanguages.keySet();
    }

    public FunctionImplementationType getFunctionImplementationType(Language language)
    {
        return supportedLanguages.get(language);
    }
}
