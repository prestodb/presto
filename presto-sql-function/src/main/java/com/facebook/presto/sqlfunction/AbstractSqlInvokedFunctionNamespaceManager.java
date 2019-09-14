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
package com.facebook.presto.sqlfunction;

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.relation.FullyQualifiedName;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractSqlInvokedFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlInvokedRegularFunction>
{
    private final ConcurrentMap<FunctionNamespaceTransactionHandle, FunctionCollection> transactions = new ConcurrentHashMap<>();

    private final LoadingCache<FullyQualifiedName, Collection<SqlInvokedRegularFunction>> functions;
    private final LoadingCache<SqlInvokedRegularFunctionHandle, FunctionMetadata> metadataByHandle;
    private final LoadingCache<SqlInvokedRegularFunctionHandle, ScalarFunctionImplementation> implementationByHandle;

    public AbstractSqlInvokedFunctionNamespaceManager(SqlInvokedFunctionNamespaceManagerConfig config)
    {
        this.functions = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<FullyQualifiedName, Collection<SqlInvokedRegularFunction>>()
                {
                    @Override
                    public Collection<SqlInvokedRegularFunction> load(FullyQualifiedName functionName)
                    {
                        Collection<SqlInvokedRegularFunction> functions = fetchFunctionsDirect(functionName);
                        for (SqlInvokedRegularFunction function : functions) {
                            metadataByHandle.put(function.getRequiredFunctionHandle(), sqlInvokedFunctionToMetadata(function));
                        }
                        return functions;
                    }
                });
        this.metadataByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlInvokedRegularFunctionHandle, FunctionMetadata>()
                {
                    @Override
                    public FunctionMetadata load(SqlInvokedRegularFunctionHandle functionHandle)
                    {
                        return fetchFunctionMetadataDirect(functionHandle);
                    }
                });
        this.implementationByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlInvokedRegularFunctionHandle, ScalarFunctionImplementation>() {
                    @Override
                    public ScalarFunctionImplementation load(SqlInvokedRegularFunctionHandle functionHandle)
                    {
                        return fetchFunctionImplementationDirect(functionHandle);
                    }
                });
    }

    protected abstract Collection<SqlInvokedRegularFunction> fetchFunctionsDirect(FullyQualifiedName functionName);

    protected abstract FunctionMetadata fetchFunctionMetadataDirect(SqlInvokedRegularFunctionHandle functionHandle);

    protected abstract ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlInvokedRegularFunctionHandle functionHandle);

    @Override
    public final FunctionNamespaceTransactionHandle beginTransaction()
    {
        UuidFunctionNamespaceTransactionHandle transactionHandle = UuidFunctionNamespaceTransactionHandle.create();
        transactions.put(transactionHandle, new FunctionCollection());
        return transactionHandle;
    }

    @Override
    public final void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional commit is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final void rollback(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional rollback is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final Collection<SqlInvokedRegularFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, FullyQualifiedName functionName)
    {
        checkArgument(transactionHandle.isPresent(), "missing transactionHandle");
        return transactions.get(transactionHandle.get()).loadAndGetFunctionsTransactional(functionName);
    }

    @Override
    public final FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        checkArgument(transactionHandle.isPresent(), "missing transactionHandle");
        SqlFunctionId functionId = new SqlFunctionId(signature.getName(), signature.getArgumentTypes());
        return transactions.get(transactionHandle.get()).getFunctionHandle(functionId);
    }

    @Override
    public final FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof SqlInvokedRegularFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        return metadataByHandle.getUnchecked((SqlInvokedRegularFunctionHandle) functionHandle);
    }

    @Override
    public final ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof SqlInvokedRegularFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        return implementationByHandle.getUnchecked((SqlInvokedRegularFunctionHandle) functionHandle);
    }

    protected static FunctionMetadata sqlInvokedFunctionToMetadata(SqlInvokedRegularFunction function)
    {
        return new FunctionMetadata(
                function.getSignature().getName(),
                function.getSignature().getArgumentTypes(),
                function.getParameterNames(),
                function.getSignature().getReturnType(),
                SCALAR,
                function.getFunctionImplementationType(),
                function.isDeterministic(),
                function.isCalledOnNullInput());
    }

    protected static ScalarFunctionImplementation sqlInvokedFunctionToImplementation(SqlInvokedRegularFunction function)
    {
        checkArgument(function.getFunctionImplementationType().equals(SQL));
        return new SqlInvokedRegularSqlFunctionImplementation(function.getBody());
    }

    private Collection<SqlInvokedRegularFunction> fetchFunctions(FullyQualifiedName functionName)
    {
        return functions.getUnchecked(functionName);
    }

    private class FunctionCollection
    {
        @GuardedBy("this")
        private final Map<FullyQualifiedName, Collection<SqlInvokedRegularFunction>> functions = new ConcurrentHashMap<>();

        @GuardedBy("this")
        private final Map<SqlFunctionId, SqlInvokedRegularFunctionHandle> functionHandles = new ConcurrentHashMap<>();

        public synchronized Collection<SqlInvokedRegularFunction> loadAndGetFunctionsTransactional(FullyQualifiedName functionName)
        {
            Collection<SqlInvokedRegularFunction> functions = this.functions.computeIfAbsent(functionName, AbstractSqlInvokedFunctionNamespaceManager.this::fetchFunctions);
            functionHandles.putAll(functions.stream().collect(toImmutableMap(SqlInvokedRegularFunction::getFunctionId, SqlInvokedRegularFunction::getRequiredFunctionHandle)));
            return functions;
        }

        public synchronized FunctionHandle getFunctionHandle(SqlFunctionId functionId)
        {
            return functionHandles.get(functionId);
        }
    }
}
