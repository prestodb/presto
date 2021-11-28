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
package com.facebook.presto.functionNamespace;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.function.ThriftScalarFunctionImplementation;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractSqlInvokedFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlInvokedFunction>
{
    private final ConcurrentMap<FunctionNamespaceTransactionHandle, FunctionCollection> transactions = new ConcurrentHashMap<>();

    private final String catalogName;
    private final SqlFunctionExecutors sqlFunctionExecutors;
    private final LoadingCache<QualifiedObjectName, Collection<SqlInvokedFunction>> functions;
    private final LoadingCache<QualifiedObjectName, UserDefinedType> userDefinedTypes;
    private final LoadingCache<SqlFunctionHandle, FunctionMetadata> metadataByHandle;
    private final LoadingCache<SqlFunctionHandle, ScalarFunctionImplementation> implementationByHandle;

    public AbstractSqlInvokedFunctionNamespaceManager(String catalogName, SqlFunctionExecutors sqlFunctionExecutors, SqlInvokedFunctionNamespaceManagerConfig config)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.sqlFunctionExecutors = requireNonNull(sqlFunctionExecutors, "sqlFunctionExecutors is null");
        requireNonNull(config, "config is null");
        this.functions = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<QualifiedObjectName, Collection<SqlInvokedFunction>>()
                {
                    @Override
                    @ParametersAreNonnullByDefault
                    public Collection<SqlInvokedFunction> load(QualifiedObjectName functionName)
                    {
                        Collection<SqlInvokedFunction> functions = fetchFunctionsDirect(functionName);
                        for (SqlInvokedFunction function : functions) {
                            metadataByHandle.put(function.getRequiredFunctionHandle(), sqlInvokedFunctionToMetadata(function));
                        }
                        return functions;
                    }
                });
        this.userDefinedTypes = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getTypeCacheExpiration().toMillis(), MILLISECONDS)
                .build(CacheLoader.from(this::fetchUserDefinedTypeDirect));

        this.metadataByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlFunctionHandle, FunctionMetadata>()
                {
                    @Override
                    @ParametersAreNonnullByDefault
                    public FunctionMetadata load(SqlFunctionHandle functionHandle)
                    {
                        return fetchFunctionMetadataDirect(functionHandle);
                    }
                });
        this.implementationByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlFunctionHandle, ScalarFunctionImplementation>()
                {
                    @Override
                    public ScalarFunctionImplementation load(SqlFunctionHandle functionHandle)
                    {
                        return fetchFunctionImplementationDirect(functionHandle);
                    }
                });
    }

    protected abstract Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName);

    protected abstract UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName);

    protected abstract FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle);

    protected abstract ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle);

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        sqlFunctionExecutors.setBlockEncodingSerde(blockEncodingSerde);
    }

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
    public final void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional rollback is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final Collection<SqlInvokedFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName)
    {
        checkCatalog(functionName);
        if (transactionHandle.isPresent()) {
            return transactions.get(transactionHandle.get()).loadAndGetFunctionsTransactional(functionName);
        }
        return fetchFunctionsDirect(functionName);
    }

    @Override
    public Optional<UserDefinedType> getUserDefinedType(QualifiedObjectName typeName)
    {
        try {
            return Optional.of(userDefinedTypes.getUnchecked(typeName));
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(NOT_FOUND.toErrorCode())) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public final FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        checkCatalog(signature.getName());
        // This is the only assumption in this class that we're dealing with sql-invoked regular function.
        SqlFunctionId functionId = new SqlFunctionId(signature.getName(), signature.getArgumentTypes());
        if (transactionHandle.isPresent()) {
            return transactions.get(transactionHandle.get()).getFunctionHandle(functionId);
        }
        FunctionCollection collection = new FunctionCollection();
        collection.loadAndGetFunctionsTransactional(signature.getName());
        return collection.getFunctionHandle(functionId);
    }

    @Override
    public final FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        return metadataByHandle.getUnchecked((SqlFunctionHandle) functionHandle);
    }

    @Override
    public final ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        return implementationByHandle.getUnchecked((SqlFunctionHandle) functionHandle);
    }

    @Override
    public CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager)
    {
        checkArgument(functionHandle instanceof SqlFunctionHandle, format("Expect SqlFunctionHandle, got %s", functionHandle.getClass()));
        FunctionMetadata functionMetadata = getFunctionMetadata(functionHandle);
        return sqlFunctionExecutors.executeFunction(
                source,
                getScalarFunctionImplementation(functionHandle),
                input,
                channels,
                functionMetadata.getArgumentTypes().stream().map(typeManager::getType).collect(toImmutableList()),
                typeManager.getType(functionMetadata.getReturnType()));
    }

    protected String getCatalogName()
    {
        return catalogName;
    }

    protected void checkCatalog(SqlFunction function)
    {
        checkCatalog(function.getSignature().getName());
    }

    protected void checkCatalog(QualifiedObjectName functionName)
    {
        checkCatalog(functionName.getCatalogSchemaName());
    }

    protected void checkCatalog(FunctionHandle functionHandle)
    {
        checkCatalog(functionHandle.getCatalogSchemaName());
    }

    protected void checkCatalog(CatalogSchemaName functionNamespace)
    {
        checkArgument(
                catalogName.equals(functionNamespace.getCatalogName()),
                "Catalog [%s] is not served by this FunctionNamespaceManager, expected: %s",
                functionNamespace.getCatalogName(),
                catalogName);
    }

    protected void refreshFunctionsCache(QualifiedObjectName functionName)
    {
        functions.refresh(functionName);
    }

    protected void checkFunctionLanguageSupported(SqlInvokedFunction function)
    {
        if (!sqlFunctionExecutors.getSupportedLanguages().contains(function.getRoutineCharacteristics().getLanguage())) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Catalog %s does not support functions implemented in language %s", catalogName, function.getRoutineCharacteristics().getLanguage()));
        }
    }

    protected FunctionMetadata sqlInvokedFunctionToMetadata(SqlInvokedFunction function)
    {
        return new FunctionMetadata(
                function.getSignature().getName(),
                function.getSignature().getArgumentTypes(),
                function.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(toImmutableList()),
                function.getSignature().getReturnType(),
                SCALAR,
                function.getRoutineCharacteristics().getLanguage(),
                getFunctionImplementationType(function),
                function.isDeterministic(),
                function.isCalledOnNullInput(),
                function.getVersion());
    }

    protected FunctionImplementationType getFunctionImplementationType(SqlInvokedFunction function)
    {
        return sqlFunctionExecutors.getFunctionImplementationType(function.getRoutineCharacteristics().getLanguage());
    }

    protected ScalarFunctionImplementation sqlInvokedFunctionToImplementation(SqlInvokedFunction function)
    {
        FunctionImplementationType implementationType = getFunctionImplementationType(function);
        switch (implementationType) {
            case SQL:
                return new SqlInvokedScalarFunctionImplementation(function.getBody());
            case THRIFT:
                checkArgument(function.getFunctionHandle().isPresent(), "Need functionHandle to get function implementation");
                return new ThriftScalarFunctionImplementation(function.getFunctionHandle().get(), function.getRoutineCharacteristics().getLanguage());
            case JAVA:
                throw new IllegalStateException(
                        format("SqlInvokedFunction %s has BUILTIN implementation type but %s cannot manage BUILTIN functions", function.getSignature().getName(), this.getClass()));
            default:
                throw new IllegalStateException(format("Unknown function implementation type: %s", implementationType));
        }
    }

    private Collection<SqlInvokedFunction> fetchFunctions(QualifiedObjectName functionName)
    {
        return functions.getUnchecked(functionName);
    }

    private class FunctionCollection
    {
        @GuardedBy("this")
        private final Map<QualifiedObjectName, Collection<SqlInvokedFunction>> functions = new ConcurrentHashMap<>();

        @GuardedBy("this")
        private final Map<SqlFunctionId, SqlFunctionHandle> functionHandles = new ConcurrentHashMap<>();

        public synchronized Collection<SqlInvokedFunction> loadAndGetFunctionsTransactional(QualifiedObjectName functionName)
        {
            Collection<SqlInvokedFunction> functions = this.functions.computeIfAbsent(functionName, AbstractSqlInvokedFunctionNamespaceManager.this::fetchFunctions);
            functionHandles.putAll(functions.stream().collect(toImmutableMap(SqlInvokedFunction::getFunctionId, SqlInvokedFunction::getRequiredFunctionHandle)));
            return functions;
        }

        public synchronized FunctionHandle getFunctionHandle(SqlFunctionId functionId)
        {
            return functionHandles.get(functionId);
        }
    }
}
