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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public abstract class BuiltInSpecialFunctionNamespaceManager
{
    protected volatile FunctionMap functions = new FunctionMap();
    protected final FunctionAndTypeManager functionAndTypeManager;
    protected final Supplier<FunctionMap> cachedFunctions =
            Suppliers.memoize(this::createFunctionMap);
    protected final LoadingCache<Signature, SpecializedFunctionKey> specializedFunctionKeyCache;
    protected final LoadingCache<SpecializedFunctionKey, ScalarFunctionImplementation> specializedScalarCache;

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

    public Collection<? extends SqlFunction> getFunctions(QualifiedObjectName functionName)
    {
        if (functions.list().isEmpty() ||
                (!functionName.getCatalogSchemaName().equals(functionAndTypeManager.getDefaultNamespace()))) {
            return emptyList();
        }
        return cachedFunctions.get().get(functionName);
    }

    public List<SqlFunction> listFunctions()
    {
        return cachedFunctions.get().list();
    }

    public abstract FunctionHandle getFunctionHandle(Signature signature);

    public abstract FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle);

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        return getScalarFunctionImplementation(((BuiltInFunctionHandle) functionHandle).getSignature());
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

    private synchronized FunctionMap createFunctionMap()
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager =
                functionAndTypeManager.getServingFunctionNamespaceManager(functionAndTypeManager.getDefaultNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for catalog '%s'", functionAndTypeManager.getDefaultNamespace().getCatalogName());
        checkForNamingConflicts(functionNamespaceManager.get().listFunctions(Optional.empty(), Optional.empty()));
        return functions;
    }

    protected abstract void checkForNamingConflicts(Collection<? extends SqlFunction> functions);

    private SpecializedFunctionKey doGetSpecializedFunctionKey(Signature signature)
    {
        return functionAndTypeManager.getSpecializedFunctionKey(signature, getFunctions(signature.getName()));
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
