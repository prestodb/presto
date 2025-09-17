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

package com.facebook.presto.metadata;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public abstract class BuiltInSpecialFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlFunction>
{
    protected volatile FunctionMap functions = new FunctionMap();
    private final FunctionAndTypeManager functionAndTypeManager;
    private final LoadingCache<Signature, SpecializedFunctionKey> specializedFunctionKeyCache;
    private final LoadingCache<SpecializedFunctionKey, ScalarFunctionImplementation> specializedScalarCache;

    public BuiltInSpecialFunctionNamespaceManager(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        specializedFunctionKeyCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::doGetSpecializedFunctionKey));
        specializedScalarCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(key -> {
                    checkArgument(
                            key.getFunction() instanceof SqlInvokedFunction,
                            "Unsupported scalar function class: %s",
                            key.getFunction().getClass());
                    return new SqlInvokedScalarFunctionImplementation(((SqlInvokedFunction) key.getFunction()).getBody());
                }));
    }

    @Override
    public Collection<SqlFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName)
    {
        return functions.get(functionName);
    }

    /**
     * likePattern / escape is not used for optimization, returning all functions.
     */
    @Override
    public Collection<SqlFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return functions.list();
    }

    @Override
    public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        return new BuiltInFunctionHandle(signature, getBuiltInFunctionKind());
    }

    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        Signature signature = ((BuiltInFunctionHandle) functionHandle).getSignature();
        SpecializedFunctionKey functionKey;
        try {
            functionKey = specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
        SqlFunction function = functionKey.getFunction();
        checkArgument(function instanceof SqlInvokedFunction, "BuiltInPluginFunctionNamespaceManager only support SqlInvokedFunctions");
        SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
        List<String> argumentNames = sqlFunction.getParameters().stream().map(Parameter::getName).collect(toImmutableList());
        return new FunctionMetadata(
                signature.getName(),
                signature.getArgumentTypes(),
                argumentNames,
                signature.getReturnType(),
                signature.getKind(),
                sqlFunction.getRoutineCharacteristics().getLanguage(),
                getDefaultFunctionMetadataImplementationType(),
                function.isDeterministic(),
                function.isCalledOnNullInput(),
                sqlFunction.getVersion(),
                sqlFunction.getComplexTypeFunctionDescriptor());
    }

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not support setting block encoding");
    }

    @Override
    public FunctionNamespaceTransactionHandle beginTransaction()
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not support setting block encoding");
    }

    @Override
    public void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not support setting block encoding");
    }

    @Override
    public void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not support setting block encoding");
    }

    @Override
    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not support setting block encoding");
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional parameterTypes, boolean exists)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not support drop function");
    }

    @Override
    public void alterFunction(QualifiedObjectName functionName, Optional parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not alter function");
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not support adding user defined types");
    }

    @Override
    public Optional<UserDefinedType> getUserDefinedType(QualifiedObjectName typeName)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not support getting user defined types");
    }

    @Override
    public CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page input, List channels, TypeManager typeManager)
    {
        throw new UnsupportedOperationException("BuiltInPluginFunctionNamespaceManager does not execute function");
    }

    protected abstract void checkForNamingConflicts(Collection<? extends SqlFunction> functions);

    protected abstract BuiltInFunctionKind getBuiltInFunctionKind();

    protected abstract FunctionImplementationType getDefaultFunctionMetadataImplementationType();

    public abstract void registerBuiltInSpecialFunctions(List<? extends SqlFunction> functions);

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        return getScalarFunctionImplementation(((BuiltInFunctionHandle) functionHandle).getSignature());
    }

    protected Collection<? extends SqlFunction> getFunctionsFromDefaultNamespace()
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager =
                functionAndTypeManager.getServingFunctionNamespaceManager(functionAndTypeManager.getDefaultNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for catalog '%s'", functionAndTypeManager.getDefaultNamespace().getCatalogName());
        return functionNamespaceManager.get().listFunctions(Optional.empty(), Optional.empty());
    }

    private synchronized FunctionMap createFunctionMap()
    {
        return functions;
    }

    private ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == SCALAR, "%s is not a scalar function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);

        try {
            return specializedScalarCache.getUnchecked(getSpecializedFunctionKey(signature));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private SpecializedFunctionKey doGetSpecializedFunctionKey(Signature signature)
    {
        return functionAndTypeManager.getSpecializedFunctionKey(signature, getFunctions(Optional.empty(), signature.getName()));
    }

    private SpecializedFunctionKey getSpecializedFunctionKey(Signature signature)
    {
        try {
            return specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }
}
