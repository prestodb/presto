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
package com.facebook.presto.functionNamespace.testing;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

@ThreadSafe
public class InMemoryFunctionNamespaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private final Map<SqlFunctionId, SqlInvokedFunction> latestFunctions = new ConcurrentHashMap<>();
    private final Map<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final Map<QualifiedObjectName, UserDefinedType> userDefinedTypes = new ConcurrentHashMap<>();

    public InMemoryFunctionNamespaceManager(String catalogName, SqlFunctionExecutors sqlFunctionExecutors, SqlInvokedFunctionNamespaceManagerConfig config)
    {
        super(catalogName, sqlFunctionExecutors, config);
    }

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        // Do not need to do anything here since InMemoryFunctionNamespaceManager cannot execute functions
    }

    @Override
    public synchronized void createFunction(SqlInvokedFunction function, boolean replace)
    {
        checkFunctionLanguageSupported(function);
        SqlFunctionId functionId = function.getFunctionId();
        if (!replace && latestFunctions.containsKey(function.getFunctionId())) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Function '%s' already exists", functionId.getId()));
        }

        SqlInvokedFunction replacedFunction = latestFunctions.get(functionId);
        long version = 1;
        if (replacedFunction != null) {
            version = parseLong(replacedFunction.getRequiredVersion()) + 1;
        }
        latestFunctions.put(functionId, function.withVersion(String.valueOf(version)));
    }

    @Override
    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new PrestoException(NOT_SUPPORTED, "Alter Function is not supported in InMemoryFunctionNamespaceManager");
    }

    @Override
    public synchronized void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(NOT_SUPPORTED, "Drop Function is not supported in InMemoryFunctionNamespaceManager");
    }

    /**
     * likePattern / escape is not used for optimization, returning all functions.
     */
    @Override
    public Collection<SqlInvokedFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return latestFunctions.values();
    }

    @Override
    public void addUserDefinedType(UserDefinedType type)
    {
        QualifiedObjectName name = type.getUserDefinedTypeName();
        checkArgument(
                !userDefinedTypes.containsKey(name),
                "Parametric type %s already registered",
                        name);
        userDefinedTypes.put(name, type);
    }

    @Override
    public Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName name)
    {
        return latestFunctions.values().stream()
                .filter(function -> function.getSignature().getName().equals(name))
                .map(InMemoryFunctionNamespaceManager::copyFunction)
                .collect(toImmutableList());
    }

    @Override
    protected UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName)
    {
        return userDefinedTypes.get(typeName);
    }

    @Override
    public FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
    {
        return fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(this::sqlInvokedFunctionToMetadata)
                .collect(onlyElement());
    }

    @Override
    protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
    {
        return fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(this::sqlInvokedFunctionToImplementation)
                .collect(onlyElement());
    }

    private static SqlInvokedFunction copyFunction(SqlInvokedFunction function)
    {
        return new SqlInvokedFunction(
                function.getSignature().getName(),
                function.getParameters(),
                function.getSignature().getReturnType(),
                function.getDescription(),
                function.getRoutineCharacteristics(),
                function.getBody(),
                function.getVersion());
    }
}
