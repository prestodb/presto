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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.QualifiedFunctionName;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.gen.CacheStatsMBean;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.BuiltInTypeRegistry;
import com.facebook.presto.type.TypeCoercer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.SystemSessionProperties.isListBuiltInFunctionsOnly;
import static com.facebook.presto.metadata.BuiltInFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.metadata.CastType.toOperatorType;
import static com.facebook.presto.metadata.FunctionResolver.constructFunctionNotFoundErrorMessage;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.EXPERIMENTAL;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FunctionAndTypeManager
        implements FunctionMetadataManager, TypeManager
{
    private final TransactionManager transactionManager;
    private final BuiltInTypeRegistry builtInTypeRegistry;
    private final BuiltInFunctionNamespaceManager builtInFunctionNamespaceManager;
    private final FunctionInvokerProvider functionInvokerProvider;
    private final Map<String, FunctionNamespaceManagerFactory> functionNamespaceManagerFactories = new ConcurrentHashMap<>();
    private final HandleResolver handleResolver;
    private final Map<String, FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManagers = new ConcurrentHashMap<>();
    private final FunctionResolver functionResolver;
    private final TypeCoercer typeCoercer;
    private final LoadingCache<FunctionResolutionCacheKey, FunctionHandle> functionCache;
    private final CacheStatsMBean cacheStatsMBean;

    @Inject
    public FunctionAndTypeManager(
            TransactionManager transactionManager,
            BlockEncodingSerde blockEncodingSerde,
            FeaturesConfig featuresConfig,
            HandleResolver handleResolver,
            Set<Type> types)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.builtInFunctionNamespaceManager = new BuiltInFunctionNamespaceManager(blockEncodingSerde, featuresConfig, this);
        this.builtInTypeRegistry = new BuiltInTypeRegistry(types, this);
        this.functionNamespaceManagers.put(DEFAULT_NAMESPACE.getCatalogName(), builtInFunctionNamespaceManager);
        this.functionInvokerProvider = new FunctionInvokerProvider(this);
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        // TODO: Provide a more encapsulated way for TransactionManager to register FunctionNamespaceManager
        transactionManager.registerFunctionNamespaceManager(DEFAULT_NAMESPACE.getCatalogName(), builtInFunctionNamespaceManager);
        this.functionCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(1000)
                .build(CacheLoader.from(key -> resolveBuiltInFunction(key.functionName, fromTypeSignatures(key.parameterTypes))));
        this.cacheStatsMBean = new CacheStatsMBean(functionCache);
        this.functionResolver = new FunctionResolver(this);
        this.typeCoercer = new TypeCoercer(featuresConfig, this);
    }

    public static FunctionAndTypeManager createTestFunctionAndTypeManager()
    {
        return new FunctionAndTypeManager(createTestTransactionManager(), new BlockEncodingManager(), new FeaturesConfig(), new HandleResolver(), ImmutableSet.of());
    }

    @Managed
    @Nested
    public CacheStatsMBean getFunctionResolutionCacheStats()
    {
        return cacheStatsMBean;
    }

    public void loadFunctionNamespaceManager(
            String functionNamespaceManagerName,
            String catalogName,
            Map<String, String> properties)
    {
        requireNonNull(functionNamespaceManagerName, "functionNamespaceManagerName is null");
        FunctionNamespaceManagerFactory factory = functionNamespaceManagerFactories.get(functionNamespaceManagerName);
        checkState(factory != null, "No factory for function namespace manager %s", functionNamespaceManagerName);
        FunctionNamespaceManager<?> functionNamespaceManager = factory.create(catalogName, properties);

        transactionManager.registerFunctionNamespaceManager(catalogName, functionNamespaceManager);
        if (functionNamespaceManagers.putIfAbsent(catalogName, functionNamespaceManager) != null) {
            throw new IllegalArgumentException(format("Function namespace manager is already registered for catalog [%s]", catalogName));
        }
    }

    @VisibleForTesting
    public void addFunctionNamespace(String catalogName, FunctionNamespaceManager functionNamespaceManager)
    {
        transactionManager.registerFunctionNamespaceManager(catalogName, functionNamespaceManager);
        if (functionNamespaceManagers.putIfAbsent(catalogName, functionNamespaceManager) != null) {
            throw new IllegalArgumentException(format("Function namespace manager is already registered for catalog [%s]", catalogName));
        }
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getFunctionNamespace());
        return functionNamespaceManager.get().getFunctionMetadata(functionHandle);
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return builtInTypeRegistry.getType(signature);
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return builtInTypeRegistry.getParameterizedType(baseTypeName, typeParameters);
    }

    @Override
    public boolean canCoerce(Type actualType, Type expectedType)
    {
        return typeCoercer.canCoerce(actualType, expectedType);
    }

    public FunctionInvokerProvider getFunctionInvokerProvider()
    {
        return functionInvokerProvider;
    }

    public void addFunctionNamespaceFactory(FunctionNamespaceManagerFactory factory)
    {
        if (functionNamespaceManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Resource group configuration manager '%s' is already registered", factory.getName()));
        }
        handleResolver.addFunctionNamespace(factory.getName(), factory.getHandleResolver());
    }

    public void registerBuiltInFunctions(List<? extends SqlFunction> functions)
    {
        builtInFunctionNamespaceManager.registerBuiltInFunctions(functions);
    }

    public List<SqlFunction> listFunctions(Session session)
    {
        Collection<FunctionNamespaceManager<?>> managers = isListBuiltInFunctionsOnly(session) ?
                ImmutableSet.of(builtInFunctionNamespaceManager) :
                functionNamespaceManagers.values();

        return managers.stream()
                .flatMap(manager -> manager.listFunctions().stream())
                .filter(function -> function.getVisibility() == PUBLIC ||
                        (function.getVisibility() == EXPERIMENTAL && SystemSessionProperties.isExperimentalFunctionsEnabled(session)))
                .collect(toImmutableList());
    }

    public Collection<? extends SqlFunction> getFunctions(Optional<TransactionId> transactionId, QualifiedFunctionName functionName)
    {
        Optional<FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getFunctionNamespace());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName));
        }

        Optional<FunctionNamespaceTransactionHandle> transactionHandle = transactionId.map(
                id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getFunctionNamespace().getCatalogName()));
        return functionNamespaceManager.get().getFunctions(transactionHandle, functionName);
    }

    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(function.getSignature().getName().getFunctionNamespace());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Cannot create function in function namespace: %s", function.getFunctionId().getFunctionName().getFunctionNamespace()));
        }
        functionNamespaceManager.get().createFunction(function, replace);
    }

    public void alterFunction(QualifiedFunctionName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getFunctionNamespace());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName));
        }
        functionNamespaceManager.get().alterFunction(functionName, parameterTypes, alterRoutineCharacteristics);
    }

    public void dropFunction(QualifiedFunctionName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getFunctionNamespace());
        if (functionNamespaceManager.isPresent()) {
            functionNamespaceManager.get().dropFunction(functionName, parameterTypes, exists);
        }
        else if (!exists) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName.getFunctionNamespace()));
        }
    }

    public static QualifiedFunctionName qualifyFunctionName(QualifiedName name)
    {
        if (!name.getPrefix().isPresent()) {
            return QualifiedFunctionName.of(DEFAULT_NAMESPACE, name.getSuffix());
        }
        if (name.getOriginalParts().size() != 3) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Non-builtin functions must be referenced by 'catalog.schema.function_name', found: %s", name));
        }
        return QualifiedFunctionName.of(new CatalogSchemaName(name.getOriginalParts().get(0), name.getOriginalParts().get(1)), name.getOriginalParts().get(2));
    }

    /**
     * Resolves a function using implicit type coercions. We enforce explicit naming for dynamic function namespaces.
     * All unqualified function names will only be resolved against the built-in static function namespace. While it is
     * possible to define an ordering (through SQL path or other means) and convention (best match / first match), in
     * reality when complicated namespaces are involved such implicit resolution might hide errors and cause confusion.
     *
     * @throws PrestoException if there are no matches or multiple matches
     */
    public FunctionHandle resolveFunction(Optional<TransactionId> transactionId, QualifiedFunctionName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        if (functionName.getFunctionNamespace().equals(DEFAULT_NAMESPACE) && parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
            return lookupCachedFunction(functionName, parameterTypes);
        }
        return resolveFunctionInternal(transactionId, functionName, parameterTypes);
    }

    public void addType(Type type)
    {
        builtInTypeRegistry.addType(type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        builtInTypeRegistry.addParametricType(parametricType);
    }

    public List<Type> getTypes()
    {
        return builtInTypeRegistry.getTypes();
    }

    public Collection<ParametricType> getParametricTypes()
    {
        return builtInTypeRegistry.getParametricTypes();
    }

    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        return typeCoercer.getCommonSuperType(firstType, secondType);
    }

    public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
    {
        return typeCoercer.isTypeOnlyCoercion(actualType, expectedType);
    }

    public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
    {
        return typeCoercer.coerceTypeBase(sourceType, resultTypeBase);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getFunctionNamespace());
        return functionNamespaceManager.get().getScalarFunctionImplementation(functionHandle);
    }

    public CompletableFuture<Block> executeFunction(FunctionHandle functionHandle, Page inputPage, List<Integer> channels)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkState(functionNamespaceManager.isPresent(), format("FunctionHandle %s should have a serving function namespace", functionHandle));
        return functionNamespaceManager.get().executeFunction(functionHandle, inputPage, channels, this);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionHandle functionHandle)
    {
        return builtInFunctionNamespaceManager.getWindowFunctionImplementation(functionHandle);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        return builtInFunctionNamespaceManager.getAggregateFunctionImplementation(functionHandle);
    }

    public BuiltInScalarFunctionImplementation getBuiltInScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        return (BuiltInScalarFunctionImplementation) builtInFunctionNamespaceManager.getScalarFunctionImplementation(functionHandle);
    }

    @VisibleForTesting
    public List<SqlFunction> listOperators()
    {
        Set<QualifiedFunctionName> operatorNames = Arrays.asList(OperatorType.values()).stream()
                .map(OperatorType::getFunctionName)
                .collect(toImmutableSet());

        return builtInFunctionNamespaceManager.listFunctions().stream()
                .filter(function -> operatorNames.contains(function.getSignature().getName()))
                .collect(toImmutableList());
    }

    public FunctionHandle resolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes)
    {
        try {
            return resolveFunction(Optional.empty(), operatorType.getFunctionName(), argumentTypes);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(
                        operatorType,
                        argumentTypes.stream()
                                .map(TypeSignatureProvider::getTypeSignature)
                                .collect(toImmutableList()));
            }
            else {
                throw e;
            }
        }
    }

    /**
     * Lookup up a function with name and fully bound types. This can only be used for builtin functions. {@link #resolveFunction(Optional, QualifiedFunctionName, List)}
     * should be used for dynamically registered functions.
     *
     * @throws PrestoException if function could not be found
     */
    public FunctionHandle lookupFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        QualifiedFunctionName functionName = qualifyFunctionName(QualifiedName.of(name));
        if (parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
            return lookupCachedFunction(functionName, parameterTypes);
        }
        Collection<? extends SqlFunction> candidates = builtInFunctionNamespaceManager.getFunctions(Optional.empty(), functionName);
        return functionResolver.lookupFunction(builtInFunctionNamespaceManager, Optional.empty(), functionName, parameterTypes, candidates);
    }

    public FunctionHandle lookupCast(CastType castType, TypeSignature fromType, TypeSignature toType)
    {
        Signature signature = new Signature(castType.getCastName(), SCALAR, emptyList(), emptyList(), toType, singletonList(fromType), false);

        try {
            builtInFunctionNamespaceManager.getScalarFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            if (castType.isOperatorType() && e.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(toOperatorType(castType), ImmutableList.of(fromType), toType);
            }
            throw e;
        }
        return builtInFunctionNamespaceManager.getFunctionHandle(Optional.empty(), signature);
    }

    private FunctionHandle resolveFunctionInternal(Optional<TransactionId> transactionId, QualifiedFunctionName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        FunctionNamespaceManager<?> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getFunctionNamespace()).orElse(null);
        if (functionNamespaceManager == null) {
            throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(functionName, parameterTypes, ImmutableList.of()));
        }

        Optional<FunctionNamespaceTransactionHandle> transactionHandle = transactionId
                .map(id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getFunctionNamespace().getCatalogName()));
        Collection<? extends SqlFunction> candidates = functionNamespaceManager.getFunctions(transactionHandle, functionName);

        return functionResolver.resolveFunction(functionNamespaceManager, transactionHandle, functionName, parameterTypes, candidates);
    }

    private FunctionHandle resolveBuiltInFunction(QualifiedFunctionName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        checkArgument(functionName.getFunctionNamespace().equals(DEFAULT_NAMESPACE), "Expect built-in functions");
        checkArgument(parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency), "Expect parameter types not to have dependency");
        return resolveFunctionInternal(Optional.empty(), functionName, parameterTypes);
    }

    private FunctionHandle lookupCachedFunction(QualifiedFunctionName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        try {
            return functionCache.getUnchecked(new FunctionResolutionCacheKey(functionName, parameterTypes));
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof PrestoException) {
                throw (PrestoException) e.getCause();
            }
            throw e;
        }
    }

    private Optional<FunctionNamespaceManager<? extends SqlFunction>> getServingFunctionNamespaceManager(CatalogSchemaName functionNamespace)
    {
        return Optional.ofNullable(functionNamespaceManagers.get(functionNamespace.getCatalogName()));
    }

    private static class FunctionResolutionCacheKey
    {
        private final QualifiedFunctionName functionName;
        private final List<TypeSignature> parameterTypes;

        private FunctionResolutionCacheKey(QualifiedFunctionName functionName, List<TypeSignatureProvider> parameterTypes)
        {
            checkArgument(parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency), "Only type signatures without dependency can be cached");
            this.functionName = requireNonNull(functionName, "functionName is null");
            this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null").stream()
                    .map(TypeSignatureProvider::getTypeSignature)
                    .collect(toImmutableList());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionName, parameterTypes);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FunctionResolutionCacheKey other = (FunctionResolutionCacheKey) obj;
            return Objects.equals(this.functionName, other.functionName) &&
                    Objects.equals(this.parameterTypes, other.parameterTypes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("functionName", functionName)
                    .add("parameterTypes", parameterTypes)
                    .toString();
        }
    }
}
