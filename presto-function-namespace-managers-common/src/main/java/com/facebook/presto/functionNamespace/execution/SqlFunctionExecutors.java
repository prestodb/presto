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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RemoteScalarFunctionImplementation;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.SqlFunctionExecutor;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlFunctionExecutors
{
    private final Map<Language, FunctionImplementationType> supportedLanguages;
    private final SqlFunctionExecutor sqlFunctionExecutor;

    @Inject
    public SqlFunctionExecutors(Map<Language, FunctionImplementationType> supportedLanguages, SqlFunctionExecutor sqlFunctionExecutor)
    {
        this.supportedLanguages = requireNonNull(supportedLanguages, "supportedLanguages is null");
        this.sqlFunctionExecutor = requireNonNull(sqlFunctionExecutor, "sqlFunctionExecutor is null");
    }

    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        sqlFunctionExecutor.setBlockEncodingSerde(blockEncodingSerde);
    }

    public Set<Language> getSupportedLanguages()
    {
        return supportedLanguages.keySet();
    }

    public FunctionImplementationType getFunctionImplementationType(Language language)
    {
        return supportedLanguages.get(language);
    }

    public CompletableFuture<SqlFunctionResult> executeFunction(String source, ScalarFunctionImplementation functionImplementation, Page input, List<Integer> channels, List<Type> argumentTypes, Type returnType)
    {
        checkArgument(functionImplementation instanceof RemoteScalarFunctionImplementation, format("Only support RemoteScalarFunctionImplementation, got %s", functionImplementation.getClass()));
        FunctionImplementationType implementationType = ((RemoteScalarFunctionImplementation) functionImplementation).getImplementationType();
        checkState(sqlFunctionExecutor.getImplementationType().equals(implementationType), format("%s SQL function executor is not setup", implementationType));
        return sqlFunctionExecutor.executeFunction(source, (RemoteScalarFunctionImplementation) functionImplementation, input, channels, argumentTypes, returnType);
    }
}
