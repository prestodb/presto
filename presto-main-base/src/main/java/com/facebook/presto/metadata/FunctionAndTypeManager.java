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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.DistinctType;
import com.facebook.presto.common.type.DistinctTypeInfo;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureBase;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.TypeWithName;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceManagerContext;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.spi.function.JavaScalarFunctionImplementation;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlFunctionSupplier;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.TableFunctionProcessorProvider;
import com.facebook.presto.spi.type.TypeManagerContext;
import com.facebook.presto.spi.type.TypeManagerFactory;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.gen.CacheStatsMBean;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeCoercer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.errorprone.annotations.ThreadSafe;
import jakarta.inject.Inject;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.facebook.presto.SystemSessionProperties.isExperimentalFunctionsEnabled;
import static com.facebook.presto.SystemSessionProperties.isListBuiltInFunctionsOnly;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInFunctionKind.PLUGIN;
import static com.facebook.presto.metadata.BuiltInFunctionKind.WORKER;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.metadata.CastType.toOperatorType;
import static com.facebook.presto.metadata.FunctionSignatureMatcher.constructFunctionNotFoundErrorMessage;
import static com.facebook.presto.metadata.FunctionSignatureMatcher.decideAndThrow;
import static com.facebook.presto.metadata.SessionFunctionHandle.SESSION_NAMESPACE;
import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.EXPERIMENTAL;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.LiteralEncoder.MAGIC_LITERAL_FUNCTION_PREFIX;
import static com.facebook.presto.sql.planner.LiteralEncoder.getMagicLiteralFunctionSignature;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

/**
 * TODO: This should not extend from FunctionMetadataManager and TypeManager
 * Functionalities relying on TypeManager and FunctionMetadataManager interfaces should rely on FunctionAndTypeResolver
 */
@ThreadSafe
public class FunctionAndTypeManager
        implements FunctionMetadataManager, TypeManager
{
    private static final Pattern DEFAULT_NAMESPACE_PREFIX_PATTERN = Pattern.compile("[a-z]+\\.[a-z]+");
    private static final Logger log = Logger.get(FunctionAndTypeManager.class);
    private final TransactionManager transactionManager;
    private final TableFunctionRegistry tableFunctionRegistry;
    private final BlockEncodingSerde blockEncodingSerde;
    private final BuiltInTypeAndFunctionNamespaceManager builtInTypeAndFunctionNamespaceManager;
    private final FunctionInvokerProvider functionInvokerProvider;
    private final Map<String, FunctionNamespaceManagerFactory> functionNamespaceManagerFactories = new ConcurrentHashMap<>();
    private final Map<String, TypeManagerFactory> typeManagerFactories = new ConcurrentHashMap<>();
    private final HandleResolver handleResolver;
    private final Map<String, FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManagers = new ConcurrentHashMap<>();
    private final Map<String, TypeManager> typeManagers = new ConcurrentHashMap<>();
    private final FunctionSignatureMatcher functionSignatureMatcher;
    private final TypeCoercer typeCoercer;
    private final LoadingCache<FunctionResolutionCacheKey, FunctionHandle> functionCache;
    private final CacheStatsMBean cacheStatsMBean;
    private final boolean nativeExecution;
    private final boolean isBuiltInSidecarFunctionsEnabled;
    private final CatalogSchemaName defaultNamespace;
    private final AtomicReference<TypeManager> servingTypeManager;
    private final AtomicReference<Supplier<Map<String, ParametricType>>> servingTypeManagerParametricTypesSupplier;
    private final BuiltInWorkerFunctionNamespaceManager builtInWorkerFunctionNamespaceManager;
    private final BuiltInPluginFunctionNamespaceManager builtInPluginFunctionNamespaceManager;
    private final ConcurrentHashMap<ConnectorId, Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider>> tableFunctionProcessorProviderMap = new ConcurrentHashMap<>();
    private final FunctionsConfig functionsConfig;
    private final Set<Type> types;

    @Inject
    public FunctionAndTypeManager(
            TransactionManager transactionManager,
            TableFunctionRegistry tableFunctionRegistry,
            BlockEncodingSerde blockEncodingSerde,
            FeaturesConfig featuresConfig,
            FunctionsConfig functionsConfig,
            HandleResolver handleResolver,
            Set<Type> types)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.tableFunctionRegistry = requireNonNull(tableFunctionRegistry, "tableFunctionRegistry is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.functionsConfig = requireNonNull(functionsConfig, "functionsConfig is null");
        this.types = requireNonNull(types, "types is null");
        this.builtInTypeAndFunctionNamespaceManager = new BuiltInTypeAndFunctionNamespaceManager(blockEncodingSerde, functionsConfig, types, this);
        this.functionNamespaceManagers.put(JAVA_BUILTIN_NAMESPACE.getCatalogName(), builtInTypeAndFunctionNamespaceManager);
        this.functionInvokerProvider = new FunctionInvokerProvider(this);
        this.typeManagers.put(JAVA_BUILTIN_NAMESPACE.getCatalogName(), builtInTypeAndFunctionNamespaceManager);
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        // TODO: Provide a more encapsulated way for TransactionManager to register FunctionNamespaceManager
        transactionManager.registerFunctionNamespaceManager(JAVA_BUILTIN_NAMESPACE.getCatalogName(), builtInTypeAndFunctionNamespaceManager);
        this.functionCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(key -> resolveBuiltInFunction(key.functionName, fromTypeSignatures(key.parameterTypes))));
        this.cacheStatsMBean = new CacheStatsMBean(functionCache);
        this.functionSignatureMatcher = new FunctionSignatureMatcher(this);
        this.typeCoercer = new TypeCoercer(functionsConfig, this);
        this.nativeExecution = featuresConfig.isNativeExecutionEnabled();
        this.isBuiltInSidecarFunctionsEnabled = featuresConfig.isBuiltInSidecarFunctionsEnabled();
        this.defaultNamespace = configureDefaultNamespace(functionsConfig.getDefaultNamespacePrefix());
        this.servingTypeManager = new AtomicReference<>(builtInTypeAndFunctionNamespaceManager);
        this.servingTypeManagerParametricTypesSupplier = new AtomicReference<>(this::getServingTypeManagerParametricTypes);
        this.builtInWorkerFunctionNamespaceManager = new BuiltInWorkerFunctionNamespaceManager(this);
        this.builtInPluginFunctionNamespaceManager = new BuiltInPluginFunctionNamespaceManager(this);
    }

    public static FunctionAndTypeManager createTestFunctionAndTypeManager()
    {
        return new FunctionAndTypeManager(
                createTestTransactionManager(),
                new TableFunctionRegistry(),
                new BlockEncodingManager(),
                new FeaturesConfig(),
                new FunctionsConfig(),
                new HandleResolver(),
                ImmutableSet.of());
    }

    public FunctionAndTypeResolver getFunctionAndTypeResolver()
    {
        return new FunctionAndTypeResolver()
        {
            // TODO: Remove the methods from the FunctionAndTypeManager class
            @Override
            public Type getType(TypeSignature signature)
            {
                return FunctionAndTypeManager.this.getType(signature);
            }

            @Override
            public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
            {
                return FunctionAndTypeManager.this.getParameterizedType(baseTypeName, typeParameters);
            }

            @Override
            public boolean canCoerce(Type actualType, Type expectedType)
            {
                return FunctionAndTypeManager.this.canCoerce(actualType, expectedType);
            }

            @Override
            public FunctionHandle resolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes)
            {
                return FunctionAndTypeManager.this.resolveOperator(operatorType, argumentTypes);
            }

            @Override
            public FunctionHandle lookupFunction(String functionName, List<TypeSignatureProvider> fromTypes)
            {
                return FunctionAndTypeManager.this.lookupFunction(functionName, fromTypes);
            }

            @Override
            public FunctionHandle resolveFunction(
                    Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions,
                    Optional<TransactionId> transactionId,
                    QualifiedObjectName functionName,
                    List<TypeSignatureProvider> parameterTypes)
            {
                return FunctionAndTypeManager.this.resolveFunction(sessionFunctions, transactionId, functionName, parameterTypes);
            }

            @Override
            public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
            {
                return FunctionAndTypeManager.this.getFunctionMetadata(functionHandle);
            }

            @Override
            public SqlFunctionSupplier getSpecializedFunctionKey(Signature signature)
            {
                return FunctionAndTypeManager.this.getSpecializedFunctionKey(signature);
            }

            @Override
            public Type instantiateParametricType(TypeSignature typeSignature)
            {
                return FunctionAndTypeManager.this.instantiateParametricType(typeSignature);
            }

            @Override
            public List<Type> getTypes()
            {
                return FunctionAndTypeManager.this.getTypes();
            }

            @Override
            public Collection<ParametricType> getParametricTypes()
            {
                return FunctionAndTypeManager.this.getParametricTypes();
            }

            @Override
            public boolean hasType(TypeSignature signature)
            {
                return FunctionAndTypeManager.this.hasType(signature);
            }

            @Override
            public Collection<SqlFunction> listBuiltInFunctions()
            {
                return FunctionAndTypeManager.this.listBuiltInFunctions();
            }

            @Override
            public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
            {
                return FunctionAndTypeManager.this.getCommonSuperType(firstType, secondType);
            }

            @Override
            public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
            {
                return FunctionAndTypeManager.this.isTypeOnlyCoercion(actualType, expectedType);
            }

            @Override
            public FunctionHandle lookupCast(String castType, Type fromType, Type toType)
            {
                return FunctionAndTypeManager.this.lookupCast(CastType.valueOf(castType), fromType, toType);
            }

            @Override
            public void validateFunctionCall(FunctionHandle functionHandle, List<?> arguments)
            {
                FunctionAndTypeManager.this.validateFunctionCall(functionHandle, arguments);
            }

            public QualifiedObjectName qualifyObjectName(QualifiedName name)
            {
                if (name.getSuffix().startsWith("$internal")) {
                    return QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, name.getSuffix());
                }
                if (!name.getPrefix().isPresent()) {
                    return QualifiedObjectName.valueOf(defaultNamespace, name.getSuffix());
                }
                if (name.getOriginalParts().size() != 3) {
                    throw new PrestoException(FUNCTION_NOT_FOUND, format("Functions that are not temporary or builtin must be referenced by 'catalog.schema.function_name', found: %s", name));
                }
                return QualifiedObjectName.valueOf(name.getParts().get(0), name.getParts().get(1), name.getParts().get(2));
            }
        };
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
            Map<String, String> properties,
            NodeManager nodeManager)
    {
        requireNonNull(functionNamespaceManagerName, "functionNamespaceManagerName is null");
        FunctionNamespaceManagerFactory factory = functionNamespaceManagerFactories.get(functionNamespaceManagerName);
        checkState(factory != null, "No factory for function namespace manager %s", functionNamespaceManagerName);
        FunctionNamespaceManager<?> functionNamespaceManager = factory.create(catalogName, properties, new FunctionNamespaceManagerContext(this, nodeManager, this));
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
        if (functionHandle.getCatalogSchemaName().equals(SESSION_NAMESPACE)) {
            return ((SessionFunctionHandle) functionHandle).getFunctionMetadata();
        }
        if (isBuiltInPluginFunctionHandle(functionHandle)) {
            return builtInPluginFunctionNamespaceManager.getFunctionMetadata(functionHandle);
        }
        if (isBuiltInWorkerFunctionHandle(functionHandle)) {
            return builtInWorkerFunctionNamespaceManager.getFunctionMetadata(functionHandle);
        }
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getCatalogSchemaName());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getCatalogSchemaName());

        return functionNamespaceManager.get().getFunctionMetadata(functionHandle);
    }

    @Override
    public Type instantiateParametricType(TypeSignature typeSignature)
    {
        return builtInTypeAndFunctionNamespaceManager.instantiateParametricType(
                typeSignature,
                this,
                servingTypeManagerParametricTypesSupplier.get().get());
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        if (signature.getTypeSignatureBase().hasStandardType()) {
            // Some info about Type has been materialized in the signature itself, so directly use it instead of fetching it
            if (signature.isDistinctType()) {
                return getDistinctType(signature.getParameters().get(0).getDistinctTypeInfo());
            }
            Type type = servingTypeManager.get().getType(signature.getStandardTypeSignature());
            if (type != null) {
                if (signature.getTypeSignatureBase().hasTypeName()) {
                    return new TypeWithName(signature.getTypeSignatureBase().getTypeName(), type);
                }
                return type;
            }
        }

        return getUserDefinedType(signature);
    }

    @Override
    public boolean hasType(TypeSignature signature)
    {
        return servingTypeManager.get().hasType(signature);
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
        String name = factory.getName();
        // SqlFunctionHandle is in SPI and used by multiple function namespace managers, use the same name for it.
        if (factory.getHandleResolver().getFunctionHandleClass().equals(SqlFunctionHandle.class)) {
            name = "sql_function_handle";
        }
        handleResolver.addFunctionNamespace(name, factory.getHandleResolver());
    }

    public TableFunctionRegistry getTableFunctionRegistry()
    {
        return tableFunctionRegistry;
    }

    public void loadTypeManager(String typeManagerName)
    {
        requireNonNull(typeManagerName, "typeManagerName is null");
        TypeManagerFactory factory = typeManagerFactories.get(typeManagerName);
        checkState(factory != null, "No factory for type manager %s", typeManagerName);
        TypeManager typeManager = factory.create(new TypeManagerContext(this));

        if (typeManagers.putIfAbsent(typeManagerName, typeManager) != null) {
            throw new IllegalArgumentException(format("Type manager [%s] is already registered", typeManager));
        }
        servingTypeManager.compareAndSet(servingTypeManager.get(), typeManager);
        // Reset the parametric types cache
        servingTypeManagerParametricTypesSupplier.set(this::getServingTypeManagerParametricTypes);
    }

    public void addTypeManagerFactory(TypeManagerFactory factory)
    {
        if (typeManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Type manager '%s' is already registered", factory.getName()));
        }
    }

    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public void registerBuiltInFunctions(List<? extends SqlFunction> functions)
    {
        builtInTypeAndFunctionNamespaceManager.registerBuiltInFunctions(functions);
    }

    public void registerWorkerFunctions(List<? extends SqlFunction> functions)
    {
        if (isBuiltInSidecarFunctionsEnabled) {
            builtInWorkerFunctionNamespaceManager.registerBuiltInSpecialFunctions(functions);
        }
    }

    @VisibleForTesting
    public void registerWorkerAggregateFunctions(List<? extends SqlFunction> aggregateFunctions)
    {
        builtInWorkerFunctionNamespaceManager.registerAggregateFunctions(aggregateFunctions);
    }

    public void registerPluginFunctions(List<? extends SqlFunction> functions)
    {
        builtInPluginFunctionNamespaceManager.registerBuiltInSpecialFunctions(functions);
    }

    public void registerConnectorFunctions(String catalogName, List<? extends SqlFunction> functions)
    {
        FunctionNamespaceManager builtInPluginFunctionNamespaceManager = functionNamespaceManagers.get(catalogName);
        if (builtInPluginFunctionNamespaceManager == null) {
            builtInPluginFunctionNamespaceManager = new BuiltInTypeAndFunctionNamespaceManager(blockEncodingSerde, functionsConfig, types, this, false);
            addFunctionNamespace(catalogName, builtInPluginFunctionNamespaceManager);
        }
        ((BuiltInTypeAndFunctionNamespaceManager) builtInPluginFunctionNamespaceManager).registerBuiltInFunctions(functions);
    }

    /**
     * likePattern / escape is an opportunistic optimization push down to function namespace managers.
     * Not all function namespace managers can handle it, thus the returned function list could
     * include functions that doesn't comply with the pattern specified. Specifically, all session
     * functions and builtin functions will always be included in the returned set. So proper handling
     * is still needed in `ShowQueriesRewrite`.
     */
    public List<SqlFunction> listFunctions(Session session, Optional<String> likePattern, Optional<String> escape)
    {
        ImmutableList.Builder<SqlFunction> functions = new ImmutableList.Builder<>();
        if (isListBuiltInFunctionsOnly(session)) {
            if (!functionNamespaceManagers.containsKey(defaultNamespace.getCatalogName())) {
                throw new PrestoException(GENERIC_USER_ERROR, format("Function namespace not found for catalog: %s", defaultNamespace.getCatalogName()));
            }
            functions.addAll(functionNamespaceManagers.get(
                            defaultNamespace.getCatalogName()).listFunctions(likePattern, escape).stream()
                    .collect(toImmutableList()));
            functions.addAll(builtInPluginFunctionNamespaceManager.listFunctions(likePattern, escape).stream().collect(toImmutableList()));
            functions.addAll(builtInWorkerFunctionNamespaceManager.listFunctions(likePattern, escape).stream().collect(toImmutableList()));
        }
        else {
            functions.addAll(SessionFunctionUtils.listFunctions(session.getSessionFunctions()));
            functions.addAll(functionNamespaceManagers.values().stream()
                    .flatMap(manager -> manager.listFunctions(likePattern, escape).stream())
                    .collect(toImmutableList()));
            functions.addAll(builtInPluginFunctionNamespaceManager.listFunctions(likePattern, escape).stream().collect(toImmutableList()));
            functions.addAll(builtInWorkerFunctionNamespaceManager.listFunctions(likePattern, escape).stream().collect(toImmutableList()));
        }

        return functions.build().stream()
                .filter(function -> function.getVisibility() == PUBLIC ||
                        (function.getVisibility() == EXPERIMENTAL && isExperimentalFunctionsEnabled(session)))
                .collect(toImmutableList());
    }

    public Collection<SqlFunction> listBuiltInFunctions()
    {
        return builtInTypeAndFunctionNamespaceManager.listFunctions(Optional.empty(), Optional.empty());
    }

    public Collection<? extends SqlFunction> getFunctions(Session session, QualifiedObjectName functionName)
    {
        if (functionName.getCatalogSchemaName().equals(JAVA_BUILTIN_NAMESPACE) &&
                SessionFunctionUtils.listFunctionNames(session.getSessionFunctions()).contains(functionName.getObjectName())) {
            return SessionFunctionUtils.getFunctions(session.getSessionFunctions(), functionName);
        }

        Optional<FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName));
        }

        Optional<FunctionNamespaceTransactionHandle> transactionHandle = session.getTransactionId().map(
                id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getCatalogName()));
        return getFunctions(functionName, transactionHandle, functionNamespaceManager.get());
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

    /**
     * Resolves a function using implicit type coercions. We enforce explicit naming for dynamic function namespaces.
     * All unqualified function names will only be resolved against the built-in static function namespace. While it is
     * possible to define an ordering (through SQL path or other means) and convention (best match / first match), in
     * reality when complicated namespaces are involved such implicit resolution might hide errors and cause confusion.
     *
     * @throws PrestoException if there are no matches or multiple matches
     */
    public FunctionHandle resolveFunction(
            Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions,
            Optional<TransactionId> transactionId,
            QualifiedObjectName functionName,
            List<TypeSignatureProvider> parameterTypes)
    {
        if (functionName.getCatalogSchemaName().equals(JAVA_BUILTIN_NAMESPACE)) {
            if (sessionFunctions.isPresent()) {
                Collection<SqlFunction> candidates = SessionFunctionUtils.getFunctions(sessionFunctions.get(), functionName);
                Optional<Signature> match = functionSignatureMatcher.match(candidates, parameterTypes, true);
                if (match.isPresent()) {
                    return SessionFunctionUtils.getFunctionHandle(sessionFunctions.get(), match.get());
                }
            }

            if (parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
                return lookupCachedFunction(functionName, parameterTypes);
            }
        }

        return resolveFunctionInternal(transactionId, functionName, parameterTypes);
    }

    public void addType(Type type)
    {
        TypeSignatureBase typeSignatureBase = type.getTypeSignature().getTypeSignatureBase();
        checkArgument(typeSignatureBase.hasStandardType(), "Expect standard types");
        builtInTypeAndFunctionNamespaceManager.addType(type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        TypeSignatureBase typeSignatureBase = parametricType.getTypeSignatureBase();
        checkArgument(typeSignatureBase.hasStandardType(), "Expect standard types");
        builtInTypeAndFunctionNamespaceManager.addParametricType(parametricType);
    }

    @VisibleForTesting
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(userDefinedType.getUserDefinedTypeName().getCatalogSchemaName());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for user defined type %", userDefinedType.getUserDefinedTypeName());
        functionNamespaceManager.get().addUserDefinedType(userDefinedType);
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
        if (functionHandle.getCatalogSchemaName().equals(SESSION_NAMESPACE)) {
            return ((SessionFunctionHandle) functionHandle).getScalarFunctionImplementation();
        }
        if (isBuiltInPluginFunctionHandle(functionHandle)) {
            return builtInPluginFunctionNamespaceManager.getScalarFunctionImplementation(functionHandle);
        }
        if (isBuiltInWorkerFunctionHandle(functionHandle)) {
            return builtInWorkerFunctionNamespaceManager.getScalarFunctionImplementation(functionHandle);
        }

        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getCatalogSchemaName());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getCatalogSchemaName());
        return functionNamespaceManager.get().getScalarFunctionImplementation(functionHandle);
    }

    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(TableFunctionHandle tableFunctionHandle)
    {
        return tableFunctionProcessorProviderMap.get(tableFunctionHandle.getConnectorId()).apply(tableFunctionHandle.getFunctionHandle());
    }

    public void addTableFunctionProcessorProvider(ConnectorId connectorId, Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> tableFunctionProcessorProvider)
    {
        if (tableFunctionProcessorProviderMap.putIfAbsent(connectorId, tableFunctionProcessorProvider) != null) {
            throw new PrestoException(ALREADY_EXISTS,
                    format("TableFuncitonProcessorProvider already exists for connectorId %s. Overwriting is not supported.", connectorId.getCatalogName()));
        }
    }

    public void removeTableFunctionProcessorProvider(ConnectorId connectorId)
    {
        tableFunctionProcessorProviderMap.remove(connectorId);
    }

    public AggregationFunctionImplementation getAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        if (isBuiltInPluginFunctionHandle(functionHandle)) {
            return builtInPluginFunctionNamespaceManager.getAggregateFunctionImplementation(functionHandle, this);
        }
        if (isBuiltInWorkerFunctionHandle(functionHandle)) {
            return builtInWorkerFunctionNamespaceManager.getAggregateFunctionImplementation(functionHandle, this);
        }

        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getCatalogSchemaName());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getCatalogSchemaName());
        return functionNamespaceManager.get().getAggregateFunctionImplementation(functionHandle, this);
    }

    public CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page inputPage, List<Integer> channels)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getCatalogSchemaName());
        checkState(functionNamespaceManager.isPresent(), format("FunctionHandle %s should have a serving function namespace", functionHandle));
        return functionNamespaceManager.get().executeFunction(source, functionHandle, inputPage, channels, this);
    }

    public void validateFunctionCall(FunctionHandle functionHandle, List<?> arguments)
    {
        // Built-in functions don't need validation
        if (functionHandle instanceof BuiltInFunctionHandle) {
            return;
        }

        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getCatalogSchemaName());
        if (functionNamespaceManager.isPresent()) {
            functionNamespaceManager.get().validateFunctionCall(functionHandle, arguments);
        }
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionHandle functionHandle)
    {
        return builtInTypeAndFunctionNamespaceManager.getWindowFunctionImplementation(functionHandle);
    }

    public JavaAggregationFunctionImplementation getJavaAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        AggregationFunctionImplementation implementation = getAggregateFunctionImplementation(functionHandle);
        checkArgument(
                implementation instanceof JavaAggregationFunctionImplementation,
                format("Implementation of function %s is not a JavaAggregationFunctionImplementationAdapter", getFunctionMetadata(functionHandle).getName()));
        return (JavaAggregationFunctionImplementation) implementation;
    }

    public JavaScalarFunctionImplementation getJavaScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        ScalarFunctionImplementation implementation = getScalarFunctionImplementation(functionHandle);
        checkArgument(
                implementation instanceof JavaScalarFunctionImplementation,
                format("Implementation of function %s is not a JavaScalarFunctionImplementation", getFunctionMetadata(functionHandle).getName()));
        return (JavaScalarFunctionImplementation) implementation;
    }

    @VisibleForTesting
    public List<SqlFunction> listOperators()
    {
        Set<QualifiedObjectName> operatorNames = Arrays.asList(OperatorType.values()).stream()
                .map(OperatorType::getFunctionName)
                .collect(toImmutableSet());

        return builtInTypeAndFunctionNamespaceManager.listFunctions(Optional.empty(), Optional.empty()).stream()
                .filter(function -> operatorNames.contains(function.getSignature().getName()))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    public Map<String, FunctionNamespaceManager<? extends SqlFunction>> getFunctionNamespaceManagers()
    {
        return ImmutableMap.copyOf(functionNamespaceManagers);
    }

    public FunctionHandle resolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes)
    {
        try {
            return resolveFunction(Optional.empty(), Optional.empty(), operatorType.getFunctionName(), argumentTypes);
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

    public boolean nullIfSpecialFormEnabled()
    {
        return !nativeExecution;
    }

    /**
     * Lookup up a function with name and fully bound types. This can only be used for builtin functions. {@link #resolveFunction(Optional, Optional, QualifiedObjectName, List)}
     * should be used for dynamically registered functions.
     *
     * @throws PrestoException if function could not be found
     */
    public FunctionHandle lookupFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        QualifiedObjectName functionName = getFunctionAndTypeResolver().qualifyObjectName(QualifiedName.of(name));
        return lookupFunction(functionName, parameterTypes);
    }

    public FunctionHandle lookupFunction(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Cannot find function namespace for function '%s'", functionName));
        }
        checkArgument(functionName.getCatalogSchemaName().equals(defaultNamespace) ||
                functionName.getCatalogSchemaName().equals(JAVA_BUILTIN_NAMESPACE), "Only default/built-in function namespace managers are allowed.");
        if (parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
            return lookupCachedFunction(functionName, parameterTypes);
        }

        return getMatchingFunctionHandle(functionName, Optional.empty(), functionNamespaceManager.get(), parameterTypes, false);
    }

    public FunctionHandle lookupCast(CastType castType, Type fromType, Type toType)
    {
        // For casts, specialize() can load more info about types, that we might not be able to get back due to
        // several layers of conversion between type and type signatures.
        // So, we manually load this info here and store it in signature which will be sent to worker.
        getCommonSuperType(fromType, toType);
        Signature signature = new Signature(castType.getCastName(), SCALAR, emptyList(), emptyList(), toType.getTypeSignature(), singletonList(fromType.getTypeSignature()), false);

        try {
            builtInTypeAndFunctionNamespaceManager.getScalarFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            if (castType.isOperatorType() && e.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(toOperatorType(castType), ImmutableList.of(fromType.getTypeSignature()), toType.getTypeSignature());
            }
            throw e;
        }
        return builtInTypeAndFunctionNamespaceManager.getFunctionHandle(Optional.empty(), signature);
    }

    public CatalogSchemaName getDefaultNamespace()
    {
        return defaultNamespace;
    }

    protected Type getType(UserDefinedType userDefinedType)
    {
        // Distinct type
        if (userDefinedType.isDistinctType()) {
            return getDistinctType(userDefinedType.getPhysicalTypeSignature().getParameters().get(0).getDistinctTypeInfo());
        }
        // Enum type or primitive type with name
        return getType(new TypeSignature(userDefinedType));
    }

    private DistinctType getDistinctType(DistinctTypeInfo distinctTypeInfo)
    {
        return new DistinctType(distinctTypeInfo,
                getType(distinctTypeInfo.getBaseType()),
                name -> (DistinctType) getType(new TypeSignature(name)));
    }

    private Type getUserDefinedType(TypeSignature signature)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(signature.getTypeSignatureBase());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for type '%s'", signature.getBase());
        UserDefinedType userDefinedType = functionNamespaceManager.get()
                .getUserDefinedType(signature.getTypeSignatureBase().getTypeName())
                .orElseThrow(() -> new IllegalArgumentException("Unknown type " + signature));
        checkArgument(userDefinedType.getPhysicalTypeSignature().getTypeSignatureBase().hasStandardType(), "A UserDefinedType must be based on static types.");
        return getType(userDefinedType);
    }

    private FunctionHandle resolveFunctionInternal(Optional<TransactionId> transactionId, QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        FunctionNamespaceManager<?> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName()).orElse(null);
        if (functionNamespaceManager == null) {
            throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(functionName, parameterTypes, ImmutableList.of()));
        }

        Optional<FunctionNamespaceTransactionHandle> transactionHandle = transactionId
                .map(id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getCatalogName()));

        if (functionNamespaceManager.canResolveFunction()) {
            return functionNamespaceManager.resolveFunction(transactionHandle, functionName, parameterTypes.stream().map(TypeSignatureProvider::getTypeSignature).collect(toImmutableList()));
        }

        try {
            return getMatchingFunctionHandle(functionName, transactionHandle, functionNamespaceManager, parameterTypes, true);
        }
        catch (PrestoException e) {
            // Could still match to a magic literal function
            if (e.getErrorCode().getCode() != StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw e;
            }
        }

        if (functionName.getObjectName().startsWith(MAGIC_LITERAL_FUNCTION_PREFIX)) {
            // extract type from function functionName
            String typeName = functionName.getObjectName().substring(MAGIC_LITERAL_FUNCTION_PREFIX.length());

            // lookup the type
            Type type = getType(parseTypeSignature(typeName));

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);

            return new BuiltInFunctionHandle(getMagicLiteralFunctionSignature(type));
        }

        throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(
                functionName, parameterTypes, getFunctions(functionName, transactionHandle, functionNamespaceManager)));
    }

    private FunctionHandle resolveBuiltInFunction(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        checkArgument(functionName.getCatalogSchemaName().equals(defaultNamespace) ||
                functionName.getCatalogSchemaName().equals(JAVA_BUILTIN_NAMESPACE), "Expect built-in/default namespace functions");
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

    public Optional<FunctionNamespaceManager<? extends SqlFunction>> getServingFunctionNamespaceManager(CatalogSchemaName functionNamespace)
    {
        return Optional.ofNullable(functionNamespaceManagers.get(functionNamespace.getCatalogName()));
    }

    private Optional<FunctionNamespaceManager<? extends SqlFunction>> getServingFunctionNamespaceManager(TypeSignatureBase typeSignatureBase)
    {
        return Optional.ofNullable(functionNamespaceManagers.get(typeSignatureBase.getTypeName().getCatalogName()));
    }

    @Override
    public SpecializedFunctionKey getSpecializedFunctionKey(Signature signature)
    {
        QualifiedObjectName functionName = signature.getName();
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Cannot find function namespace for signature '%s'", functionName));
        }

        Collection<? extends SqlFunction> candidates = functionNamespaceManager.get().getFunctions(Optional.empty(), functionName);

        return getSpecializedFunctionKey(signature, candidates);
    }

    public SpecializedFunctionKey getSpecializedFunctionKey(Signature signature, Collection<? extends SqlFunction> candidates)
    {
        // search for exact match
        Type returnType = getType(signature.getReturnType());
        List<TypeSignatureProvider> argumentTypeSignatureProviders = fromTypeSignatures(signature.getArgumentTypes());
        for (SqlFunction candidate : candidates) {
            Optional<BoundVariables> boundVariables = new SignatureBinder(this, candidate.getSignature(), false)
                    .bindVariables(argumentTypeSignatureProviders, returnType);
            if (boundVariables.isPresent()) {
                return new SpecializedFunctionKey(candidate, boundVariables.get(), argumentTypeSignatureProviders.size());
            }
        }

        // TODO: hack because there could be "type only" coercions (which aren't necessarily included as implicit casts),
        // so do a second pass allowing "type only" coercions
        List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), this);
        for (SqlFunction candidate : candidates) {
            SignatureBinder binder = new SignatureBinder(this, candidate.getSignature(), true);
            Optional<BoundVariables> boundVariables = binder.bindVariables(argumentTypeSignatureProviders, returnType);
            if (!boundVariables.isPresent()) {
                continue;
            }
            Signature boundSignature = applyBoundVariables(candidate.getSignature(), boundVariables.get(), argumentTypes.size());

            if (!isTypeOnlyCoercion(getType(boundSignature.getReturnType()), returnType)) {
                continue;
            }
            boolean nonTypeOnlyCoercion = false;
            for (int i = 0; i < argumentTypes.size(); i++) {
                Type expectedType = getType(boundSignature.getArgumentTypes().get(i));
                if (!isTypeOnlyCoercion(argumentTypes.get(i), expectedType)) {
                    nonTypeOnlyCoercion = true;
                    break;
                }
            }
            if (nonTypeOnlyCoercion) {
                continue;
            }

            return new SpecializedFunctionKey(candidate, boundVariables.get(), argumentTypes.size());
        }

        // One final check for magic literal functions.
        // Magic literal functions are only present in the JAVA_BUILTIN_NAMESPACE function namespace.
        if (!signature.getName().getCatalogSchemaName().equals(JAVA_BUILTIN_NAMESPACE)) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
        }
        return builtInTypeAndFunctionNamespaceManager.doGetSpecializedFunctionKeyForMagicLiteralFunctions(signature, this);
    }

    public BuiltInPluginFunctionNamespaceManager getBuiltInPluginFunctionNamespaceManager()
    {
        return builtInPluginFunctionNamespaceManager;
    }

    private CatalogSchemaName configureDefaultNamespace(String defaultNamespacePrefixString)
    {
        if (!defaultNamespacePrefixString.matches(DEFAULT_NAMESPACE_PREFIX_PATTERN.pattern())) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Default namespace prefix string should be in the form of 'catalog.schema', found: %s", defaultNamespacePrefixString));
        }
        String[] catalogSchemaNameString = defaultNamespacePrefixString.split("\\.");
        return new CatalogSchemaName(catalogSchemaNameString[0], catalogSchemaNameString[1]);
    }

    private Map<String, ParametricType> getServingTypeManagerParametricTypes()
    {
        return servingTypeManager.get().getParametricTypes().stream()
                .collect(toImmutableMap(ParametricType::getName, parametricType -> parametricType));
    }

    private Collection<? extends SqlFunction> getFunctions(
            QualifiedObjectName functionName,
            Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle,
            FunctionNamespaceManager<?> functionNamespaceManager)
    {
        return ImmutableList.<SqlFunction>builder()
                .addAll(functionNamespaceManager.getFunctions(transactionHandle, functionName))
                .addAll(builtInPluginFunctionNamespaceManager.getFunctions(transactionHandle, functionName))
                .addAll(builtInWorkerFunctionNamespaceManager.getFunctions(transactionHandle, functionName))
                .build();
    }

    /**
     * Gets the function handle of the function if there is a match. We enforce explicit naming for dynamic function namespaces.
     * All unqualified function names will only be resolved against the built-in default function namespace. We get all the candidates
     * from the current default namespace and additionally all the candidates from builtInPluginFunctionNamespaceManager and
     * builtInWorkerFunctionNamespaceManager.
     *
     * @throws PrestoException if there are no matches or multiple matches
     */
    private FunctionHandle getMatchingFunctionHandle(
            QualifiedObjectName functionName,
            Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle,
            FunctionNamespaceManager<?> functionNamespaceManager,
            List<TypeSignatureProvider> parameterTypes,
            boolean coercionAllowed)
    {
        boolean foundMatch = false;
        List<SemanticException> exceptions = new ArrayList<>();
        List<SqlFunction> allCandidates = new ArrayList<>();
        Optional<Signature> matchingDefaultFunctionSignature = Optional.empty();
        Optional<Signature> matchingPluginFunctionSignature = Optional.empty();
        Optional<Signature> matchingWorkerFunctionSignature = Optional.empty();

        try {
            Collection<? extends SqlFunction> defaultCandidates = functionNamespaceManager.getFunctions(transactionHandle, functionName);
            allCandidates.addAll(defaultCandidates);
            matchingDefaultFunctionSignature =
                    getMatchingFunction(defaultCandidates, parameterTypes, coercionAllowed);
            if (matchingDefaultFunctionSignature.isPresent()) {
                foundMatch = true;
            }
        }
        catch (SemanticException e) {
            exceptions.add(e);
        }

        try {
            Collection<? extends SqlFunction> pluginCandidates = builtInPluginFunctionNamespaceManager.getFunctions(transactionHandle, functionName);
            allCandidates.addAll(pluginCandidates);
            matchingPluginFunctionSignature =
                    getMatchingFunction(pluginCandidates, parameterTypes, coercionAllowed);
            if (matchingPluginFunctionSignature.isPresent()) {
                foundMatch = true;
            }
        }
        catch (SemanticException e) {
            exceptions.add(e);
        }

        try {
            Collection<? extends SqlFunction> workerCandidates = builtInWorkerFunctionNamespaceManager.getFunctions(transactionHandle, functionName);
            allCandidates.addAll(workerCandidates);
            matchingWorkerFunctionSignature =
                    getMatchingFunction(workerCandidates, parameterTypes, coercionAllowed);
            if (matchingWorkerFunctionSignature.isPresent()) {
                foundMatch = true;
            }
        }
        catch (SemanticException e) {
            exceptions.add(e);
        }

        if (!foundMatch && !exceptions.isEmpty()) {
            decideAndThrow(exceptions,
                    allCandidates.stream().findFirst()
                            .map(function -> function.getSignature().getName().getObjectName())
                            .orElse(""));
        }

        if (matchingDefaultFunctionSignature.isPresent() && matchingPluginFunctionSignature.isPresent()) {
            throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, format("Function '%s' has two matching signatures. Please specify parameter types. \n" +
                    "First match : '%s', Second match: '%s'", functionName, matchingDefaultFunctionSignature.get(), matchingPluginFunctionSignature.get()));
        }

        if (matchingDefaultFunctionSignature.isPresent() && matchingWorkerFunctionSignature.isPresent()) {
            FunctionHandle defaultFunctionHandle = functionNamespaceManager.getFunctionHandle(transactionHandle, matchingDefaultFunctionSignature.get());
            FunctionHandle workerFunctionHandle = builtInWorkerFunctionNamespaceManager.getFunctionHandle(transactionHandle, matchingWorkerFunctionSignature.get());

            if (functionNamespaceManager.getFunctionMetadata(defaultFunctionHandle).getImplementationType().equals(FunctionImplementationType.JAVA)) {
                return defaultFunctionHandle;
            }
            if (functionNamespaceManager.getFunctionMetadata(defaultFunctionHandle).getImplementationType().equals(FunctionImplementationType.SQL)) {
                return workerFunctionHandle;
            }
        }

        if (matchingPluginFunctionSignature.isPresent() && matchingWorkerFunctionSignature.isPresent()) {
            // built in plugin function namespace manager always has SQL as implementation type
            return builtInWorkerFunctionNamespaceManager.getFunctionHandle(transactionHandle, matchingWorkerFunctionSignature.get());
        }

        if (matchingWorkerFunctionSignature.isPresent()) {
            return builtInWorkerFunctionNamespaceManager.getFunctionHandle(transactionHandle, matchingWorkerFunctionSignature.get());
        }

        if (matchingPluginFunctionSignature.isPresent()) {
            return builtInPluginFunctionNamespaceManager.getFunctionHandle(transactionHandle, matchingPluginFunctionSignature.get());
        }

        if (matchingDefaultFunctionSignature.isPresent()) {
            return functionNamespaceManager.getFunctionHandle(transactionHandle, matchingDefaultFunctionSignature.get());
        }

        throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(functionName, parameterTypes,
                getFunctions(functionName, transactionHandle, functionNamespaceManager)));
    }

    private Optional<Signature> getMatchingFunction(
            Collection<? extends SqlFunction> candidates,
            List<TypeSignatureProvider> parameterTypes,
            boolean coercionAllowed)
    {
        return functionSignatureMatcher.match(candidates, parameterTypes, coercionAllowed);
    }

    private boolean isBuiltInPluginFunctionHandle(FunctionHandle functionHandle)
    {
        return (functionHandle instanceof BuiltInFunctionHandle) && ((BuiltInFunctionHandle) functionHandle).getBuiltInFunctionKind().equals(PLUGIN);
    }

    private boolean isBuiltInWorkerFunctionHandle(FunctionHandle functionHandle)
    {
        return (functionHandle instanceof BuiltInFunctionHandle) && ((BuiltInFunctionHandle) functionHandle).getBuiltInFunctionKind().equals(WORKER);
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
