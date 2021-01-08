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
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureBase;
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
import com.facebook.presto.type.MapParametricType;
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

import java.util.ArrayList;
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
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
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
import static com.google.common.base.Throwables.throwIfUnchecked;
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
    private final BlockEncodingSerde blockEncodingSerde;
    private final BuiltInTypeAndFunctionNamespaceManager builtInTypeAndFunctionNamespaceManager;
    private final FunctionInvokerProvider functionInvokerProvider;
    private final Map<String, FunctionNamespaceManagerFactory> functionNamespaceManagerFactories = new ConcurrentHashMap<>();
    private final HandleResolver handleResolver;
    private final Map<String, FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManagers = new ConcurrentHashMap<>();
    private final FunctionResolver functionResolver;
    private final TypeCoercer typeCoercer;
    private final LoadingCache<FunctionResolutionCacheKey, FunctionHandle> functionCache;
    private final LoadingCache<TypeSignature, Type> parametricTypeCache;
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
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.builtInTypeAndFunctionNamespaceManager = new BuiltInTypeAndFunctionNamespaceManager(blockEncodingSerde, featuresConfig, types, this);
        this.functionNamespaceManagers.put(DEFAULT_NAMESPACE.getCatalogName(), builtInTypeAndFunctionNamespaceManager);
        this.functionInvokerProvider = new FunctionInvokerProvider(this);
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        // TODO: Provide a more encapsulated way for TransactionManager to register FunctionNamespaceManager
        transactionManager.registerFunctionNamespaceManager(DEFAULT_NAMESPACE.getCatalogName(), builtInTypeAndFunctionNamespaceManager);
        this.functionCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(1000)
                .build(CacheLoader.from(key -> resolveBuiltInFunction(key.functionName, fromTypeSignatures(key.parameterTypes))));
        this.parametricTypeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(typeSignature -> instantiateParametricType(typeSignature, builtInTypeAndFunctionNamespaceManager)));

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
        functionNamespaceManager.setBlockEncodingSerde(blockEncodingSerde);

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
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getCatalogSchemaName());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getCatalogSchemaName());
        return functionNamespaceManager.get().getFunctionMetadata(functionHandle);
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        if (signature.getTypeSignatureBase().isStandardType()) {
            Optional<Type> type = builtInTypeAndFunctionNamespaceManager.getType(signature);
            if (type.isPresent()) {
                return type.get();
            }
            try {
                return parametricTypeCache.getUnchecked(signature);
            }
            catch (UncheckedExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }

        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(signature.getTypeSignatureBase());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for type '%s'", signature.getBase());
        return instantiateParametricType(signature, functionNamespaceManager.get());
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters));
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
        builtInTypeAndFunctionNamespaceManager.registerBuiltInFunctions(functions);
    }

    public List<SqlFunction> listFunctions(Session session)
    {
        Collection<FunctionNamespaceManager<?>> managers = isListBuiltInFunctionsOnly(session) ?
                ImmutableSet.of(builtInTypeAndFunctionNamespaceManager) :
                functionNamespaceManagers.values();

        return managers.stream()
                .flatMap(manager -> manager.listFunctions().stream())
                .filter(function -> function.getVisibility() == PUBLIC ||
                        (function.getVisibility() == EXPERIMENTAL && SystemSessionProperties.isExperimentalFunctionsEnabled(session)))
                .collect(toImmutableList());
    }

    public Collection<? extends SqlFunction> getFunctions(Optional<TransactionId> transactionId, QualifiedObjectName functionName)
    {
        Optional<FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName));
        }

        Optional<FunctionNamespaceTransactionHandle> transactionHandle = transactionId.map(
                id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getCatalogName()));
        return functionNamespaceManager.get().getFunctions(transactionHandle, functionName);
    }

    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(function.getSignature().getName().getCatalogSchemaName());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Cannot create function in function namespace: %s", function.getFunctionId().getFunctionName().getCatalogSchemaName()));
        }
        functionNamespaceManager.get().createFunction(function, replace);
    }

    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName));
        }
        functionNamespaceManager.get().alterFunction(functionName, parameterTypes, alterRoutineCharacteristics);
    }

    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName());
        if (functionNamespaceManager.isPresent()) {
            functionNamespaceManager.get().dropFunction(functionName, parameterTypes, exists);
        }
        else if (!exists) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName.getCatalogSchemaName()));
        }
    }

    public static QualifiedObjectName qualifyObjectName(QualifiedName name)
    {
        if (!name.getPrefix().isPresent()) {
            return QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, name.getSuffix());
        }
        if (name.getOriginalParts().size() != 3) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Non-builtin functions must be referenced by 'catalog.schema.function_name', found: %s", name));
        }
        return QualifiedObjectName.valueOf(name.getParts().get(0), name.getParts().get(1), name.getParts().get(2));
    }

    /**
     * Resolves a function using implicit type coercions. We enforce explicit naming for dynamic function namespaces.
     * All unqualified function names will only be resolved against the built-in static function namespace. While it is
     * possible to define an ordering (through SQL path or other means) and convention (best match / first match), in
     * reality when complicated namespaces are involved such implicit resolution might hide errors and cause confusion.
     *
     * @throws PrestoException if there are no matches or multiple matches
     */
    public FunctionHandle resolveFunction(Optional<TransactionId> transactionId, QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        if (functionName.getCatalogSchemaName().equals(DEFAULT_NAMESPACE) && parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
            return lookupCachedFunction(functionName, parameterTypes);
        }
        return resolveFunctionInternal(transactionId, functionName, parameterTypes);
    }

    public void addType(Type type)
    {
        TypeSignatureBase typeSignatureBase = type.getTypeSignature().getTypeSignatureBase();
        checkArgument(typeSignatureBase.isStandardType(), "Expect standard types");
        builtInTypeAndFunctionNamespaceManager.addType(type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        TypeSignatureBase typeSignatureBase = parametricType.getTypeSignatureBase();
        if (typeSignatureBase.isStandardType()) {
            builtInTypeAndFunctionNamespaceManager.addParametricType(parametricType);
        }
        else {
            Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(typeSignatureBase);
            checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for parametric type %", parametricType);
            functionNamespaceManager.get().addParametricType(parametricType);
        }
    }

    public List<Type> getTypes()
    {
        return builtInTypeAndFunctionNamespaceManager.getTypes();
    }

    public Collection<ParametricType> getParametricTypes()
    {
        return builtInTypeAndFunctionNamespaceManager.getParametricTypes();
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
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getCatalogSchemaName());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getCatalogSchemaName());
        return functionNamespaceManager.get().getScalarFunctionImplementation(functionHandle);
    }

    public CompletableFuture<Block> executeFunction(FunctionHandle functionHandle, Page inputPage, List<Integer> channels)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getCatalogSchemaName());
        checkState(functionNamespaceManager.isPresent(), format("FunctionHandle %s should have a serving function namespace", functionHandle));
        return functionNamespaceManager.get().executeFunction(functionHandle, inputPage, channels, this);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionHandle functionHandle)
    {
        return builtInTypeAndFunctionNamespaceManager.getWindowFunctionImplementation(functionHandle);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        return builtInTypeAndFunctionNamespaceManager.getAggregateFunctionImplementation(functionHandle);
    }

    public BuiltInScalarFunctionImplementation getBuiltInScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        return (BuiltInScalarFunctionImplementation) builtInTypeAndFunctionNamespaceManager.getScalarFunctionImplementation(functionHandle);
    }

    @VisibleForTesting
    public List<SqlFunction> listOperators()
    {
        Set<QualifiedObjectName> operatorNames = Arrays.asList(OperatorType.values()).stream()
                .map(OperatorType::getFunctionName)
                .collect(toImmutableSet());

        return builtInTypeAndFunctionNamespaceManager.listFunctions().stream()
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
     * Lookup up a function with name and fully bound types. This can only be used for builtin functions. {@link #resolveFunction(Optional, QualifiedObjectName, List)}
     * should be used for dynamically registered functions.
     *
     * @throws PrestoException if function could not be found
     */
    public FunctionHandle lookupFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        QualifiedObjectName functionName = qualifyObjectName(QualifiedName.of(name));
        if (parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
            return lookupCachedFunction(functionName, parameterTypes);
        }
        Collection<? extends SqlFunction> candidates = builtInTypeAndFunctionNamespaceManager.getFunctions(Optional.empty(), functionName);
        return functionResolver.lookupFunction(builtInTypeAndFunctionNamespaceManager, Optional.empty(), functionName, parameterTypes, candidates);
    }

    public FunctionHandle lookupCast(CastType castType, TypeSignature fromType, TypeSignature toType)
    {
        Signature signature = new Signature(castType.getCastName(), SCALAR, emptyList(), emptyList(), toType, singletonList(fromType), false);

        try {
            builtInTypeAndFunctionNamespaceManager.getScalarFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            if (castType.isOperatorType() && e.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(toOperatorType(castType), ImmutableList.of(fromType), toType);
            }
            throw e;
        }
        return builtInTypeAndFunctionNamespaceManager.getFunctionHandle(Optional.empty(), signature);
    }

    private FunctionHandle resolveFunctionInternal(Optional<TransactionId> transactionId, QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        FunctionNamespaceManager<?> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName()).orElse(null);
        if (functionNamespaceManager == null) {
            throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(functionName, parameterTypes, ImmutableList.of()));
        }

        Optional<FunctionNamespaceTransactionHandle> transactionHandle = transactionId
                .map(id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getCatalogName()));
        Collection<? extends SqlFunction> candidates = functionNamespaceManager.getFunctions(transactionHandle, functionName);

        return functionResolver.resolveFunction(functionNamespaceManager, transactionHandle, functionName, parameterTypes, candidates);
    }

    private FunctionHandle resolveBuiltInFunction(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        checkArgument(functionName.getCatalogSchemaName().equals(DEFAULT_NAMESPACE), "Expect built-in functions");
        checkArgument(parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency), "Expect parameter types not to have dependency");
        return resolveFunctionInternal(Optional.empty(), functionName, parameterTypes);
    }

    private FunctionHandle lookupCachedFunction(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
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

    private Optional<FunctionNamespaceManager<? extends SqlFunction>> getServingFunctionNamespaceManager(TypeSignatureBase typeSignatureBase)
    {
        return Optional.ofNullable(functionNamespaceManagers.get(typeSignatureBase.getQualifiedObjectName().getCatalogName()));
    }

    private Type instantiateParametricType(TypeSignature signature, FunctionNamespaceManager<?> functionNamespaceManager)
    {
        List<TypeParameter> parameters = new ArrayList<>();

        for (TypeSignatureParameter parameter : signature.getParameters()) {
            TypeParameter typeParameter = TypeParameter.of(parameter, this);
            parameters.add(typeParameter);
        }

        Optional<ParametricType> type = functionNamespaceManager.getParametricType(signature);
        if (!type.isPresent()) {
            throw new IllegalArgumentException("Unknown type " + signature);
        }

        ParametricType parametricType = type.get();
        if (parametricType instanceof MapParametricType) {
            return ((MapParametricType) parametricType).createType(this, parameters);
        }

        Type instantiatedType = parametricType.createType(parameters);

        // TODO: reimplement this check? Currently "varchar(Integer.MAX_VALUE)" fails with "varchar"
        //checkState(instantiatedType.equalsSignature(signature), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType, signature);
        return instantiatedType;
    }

    private static class FunctionResolutionCacheKey
    {
        private final QualifiedObjectName functionName;
        private final List<TypeSignature> parameterTypes;

        private FunctionResolutionCacheKey(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
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
