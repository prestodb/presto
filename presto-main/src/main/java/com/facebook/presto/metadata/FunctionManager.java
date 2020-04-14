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
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.QualifiedFunctionName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
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
import com.facebook.presto.type.TypeRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isListBuiltInFunctionsOnly;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.metadata.CastType.toOperatorType;
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
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FunctionManager
        implements FunctionMetadataManager
{
    private final TypeManager typeManager;
    private final TransactionManager transactionManager;
    private final BuiltInFunctionNamespaceManager builtInFunctionNamespaceManager;
    private final FunctionInvokerProvider functionInvokerProvider;
    private final Map<String, FunctionNamespaceManagerFactory> functionNamespaceManagerFactories = new ConcurrentHashMap<>();
    private final HandleResolver handleResolver;
    private final Map<String, FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManagers = new ConcurrentHashMap<>();
    private final LoadingCache<FunctionResolutionCacheKey, FunctionHandle> functionCache;
    private final CacheStatsMBean cacheStatsMBean;

    @Inject
    public FunctionManager(
            TypeManager typeManager,
            TransactionManager transactionManager,
            BlockEncodingSerde blockEncodingSerde,
            FeaturesConfig featuresConfig,
            HandleResolver handleResolver)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.builtInFunctionNamespaceManager = new BuiltInFunctionNamespaceManager(typeManager, blockEncodingSerde, featuresConfig, this);
        this.functionNamespaceManagers.put(DEFAULT_NAMESPACE.getCatalogName(), builtInFunctionNamespaceManager);
        this.functionInvokerProvider = new FunctionInvokerProvider(this);
        this.handleResolver = handleResolver;
        if (typeManager instanceof TypeRegistry) {
            ((TypeRegistry) typeManager).setFunctionManager(this);
        }
        // TODO: Provide a more encapsulated way for TransactionManager to register FunctionNamespaceManager
        transactionManager.registerFunctionNamespaceManager(DEFAULT_NAMESPACE.getCatalogName(), builtInFunctionNamespaceManager);
        this.functionCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(1000)
                .build(CacheLoader.from(key -> resolveBuiltInFunction(key.functionName, fromTypeSignatures(key.parameterTypes))));
        this.cacheStatsMBean = new CacheStatsMBean(functionCache);
    }

    @VisibleForTesting
    public FunctionManager(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, FeaturesConfig featuresConfig)
    {
        // TODO: Convert this constructor to a function in the testing package
        this(typeManager, createTestTransactionManager(), blockEncodingSerde, featuresConfig, new HandleResolver());
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

    private FunctionHandle resolveFunctionInternal(Optional<TransactionId> transactionId, QualifiedFunctionName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        FunctionNamespaceManager<?> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getFunctionNamespace()).orElse(null);
        if (functionNamespaceManager == null) {
            throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(functionName, parameterTypes, ImmutableList.of()));
        }

        Optional<FunctionNamespaceTransactionHandle> transactionHandle = transactionId
                .map(id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getFunctionNamespace().getCatalogName()));
        Collection<? extends SqlFunction> candidates = functionNamespaceManager.getFunctions(transactionHandle, functionName);

        try {
            return lookupFunction(functionNamespaceManager, transactionHandle, functionName, parameterTypes, candidates);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() != FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw e;
            }
        }

        Optional<Signature> match = matchFunctionWithCoercion(candidates, parameterTypes);
        if (match.isPresent()) {
            return functionNamespaceManager.getFunctionHandle(transactionHandle, match.get());
        }

        if (functionName.getFunctionName().startsWith(MAGIC_LITERAL_FUNCTION_PREFIX)) {
            // extract type from function functionName
            String typeName = functionName.getFunctionName().substring(MAGIC_LITERAL_FUNCTION_PREFIX.length());

            // lookup the type
            Type type = typeManager.getType(parseTypeSignature(typeName));

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);

            return new BuiltInFunctionHandle(getMagicLiteralFunctionSignature(type));
        }

        throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(functionName, parameterTypes, candidates));
    }

    private FunctionHandle resolveBuiltInFunction(QualifiedFunctionName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        checkArgument(functionName.getFunctionNamespace().equals(DEFAULT_NAMESPACE), "Expect built-in functions");
        checkArgument(parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency), "Expect parameter types not to have dependency");
        return resolveFunctionInternal(Optional.empty(), functionName, parameterTypes);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getFunctionNamespace());
        return functionNamespaceManager.get().getFunctionMetadata(functionHandle);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getFunctionNamespace());
        return functionNamespaceManager.get().getScalarFunctionImplementation(functionHandle);
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
        return lookupFunction(builtInFunctionNamespaceManager, Optional.empty(), functionName, parameterTypes, candidates);
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

    private FunctionHandle lookupFunction(
            FunctionNamespaceManager<?> functionNamespaceManager,
            Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle,
            QualifiedFunctionName functionName,
            List<TypeSignatureProvider> parameterTypes,
            Collection<? extends SqlFunction> candidates)
    {
        List<SqlFunction> exactCandidates = candidates.stream()
                .filter(function -> function.getSignature().getTypeVariableConstraints().isEmpty())
                .collect(Collectors.toList());

        Optional<Signature> match = matchFunctionExact(exactCandidates, parameterTypes);
        if (match.isPresent()) {
            return functionNamespaceManager.getFunctionHandle(transactionHandle, match.get());
        }

        List<SqlFunction> genericCandidates = candidates.stream()
                .filter(function -> !function.getSignature().getTypeVariableConstraints().isEmpty())
                .collect(Collectors.toList());

        match = matchFunctionExact(genericCandidates, parameterTypes);
        if (match.isPresent()) {
            return functionNamespaceManager.getFunctionHandle(transactionHandle, match.get());
        }

        throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(functionName, parameterTypes, candidates));
    }

    private Optional<FunctionNamespaceManager<? extends SqlFunction>> getServingFunctionNamespaceManager(CatalogSchemaName functionNamespace)
    {
        return Optional.ofNullable(functionNamespaceManagers.get(functionNamespace.getCatalogName()));
    }

    private String constructFunctionNotFoundErrorMessage(QualifiedFunctionName functionName, List<TypeSignatureProvider> parameterTypes, Collection<? extends SqlFunction> candidates)
    {
        String name = toConciseFunctionName(functionName);
        List<String> expectedParameters = new ArrayList<>();
        for (SqlFunction function : candidates) {
            expectedParameters.add(format("%s(%s) %s",
                    name,
                    Joiner.on(", ").join(function.getSignature().getArgumentTypes()),
                    Joiner.on(", ").join(function.getSignature().getTypeVariableConstraints())));
        }
        String parameters = Joiner.on(", ").join(parameterTypes);
        String message = format("Function %s not registered", name);
        if (!expectedParameters.isEmpty()) {
            String expected = Joiner.on(", ").join(expectedParameters);
            message = format("Unexpected parameters (%s) for function %s. Expected: %s", parameters, name, expected);
        }
        return message;
    }

    private String toConciseFunctionName(QualifiedFunctionName functionName)
    {
        if (functionName.getFunctionNamespace().equals(DEFAULT_NAMESPACE)) {
            return functionName.getFunctionName();
        }
        return functionName.toString();
    }

    private Optional<Signature> matchFunctionExact(List<SqlFunction> candidates, List<TypeSignatureProvider> actualParameters)
    {
        return matchFunction(candidates, actualParameters, false);
    }

    private Optional<Signature> matchFunctionWithCoercion(Collection<? extends SqlFunction> candidates, List<TypeSignatureProvider> actualParameters)
    {
        return matchFunction(candidates, actualParameters, true);
    }

    private Optional<Signature> matchFunction(Collection<? extends SqlFunction> candidates, List<TypeSignatureProvider> parameters, boolean coercionAllowed)
    {
        List<ApplicableFunction> applicableFunctions = identifyApplicableFunctions(candidates, parameters, coercionAllowed);
        if (applicableFunctions.isEmpty()) {
            return Optional.empty();
        }

        if (coercionAllowed) {
            applicableFunctions = selectMostSpecificFunctions(applicableFunctions, parameters);
            checkState(!applicableFunctions.isEmpty(), "at least single function must be left");
        }

        if (applicableFunctions.size() == 1) {
            return Optional.of(getOnlyElement(applicableFunctions).getBoundSignature());
        }

        StringBuilder errorMessageBuilder = new StringBuilder();
        errorMessageBuilder.append("Could not choose a best candidate operator. Explicit type casts must be added.\n");
        errorMessageBuilder.append("Candidates are:\n");
        for (ApplicableFunction function : applicableFunctions) {
            errorMessageBuilder.append("\t * ");
            errorMessageBuilder.append(function.getBoundSignature().toString());
            errorMessageBuilder.append("\n");
        }
        throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, errorMessageBuilder.toString());
    }

    private List<ApplicableFunction> identifyApplicableFunctions(Collection<? extends SqlFunction> candidates, List<TypeSignatureProvider> actualParameters, boolean allowCoercion)
    {
        ImmutableList.Builder<ApplicableFunction> applicableFunctions = ImmutableList.builder();
        for (SqlFunction function : candidates) {
            Signature declaredSignature = function.getSignature();
            Optional<Signature> boundSignature = new SignatureBinder(typeManager, declaredSignature, allowCoercion)
                    .bind(actualParameters);
            if (boundSignature.isPresent()) {
                applicableFunctions.add(new ApplicableFunction(declaredSignature, boundSignature.get(), function.isCalledOnNullInput()));
            }
        }
        return applicableFunctions.build();
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> applicableFunctions, List<TypeSignatureProvider> parameters)
    {
        checkArgument(!applicableFunctions.isEmpty());

        List<ApplicableFunction> mostSpecificFunctions = selectMostSpecificFunctions(applicableFunctions);
        if (mostSpecificFunctions.size() <= 1) {
            return mostSpecificFunctions;
        }

        Optional<List<Type>> optionalParameterTypes = toTypes(parameters, typeManager);
        if (!optionalParameterTypes.isPresent()) {
            // give up and return all remaining matches
            return mostSpecificFunctions;
        }

        List<Type> parameterTypes = optionalParameterTypes.get();
        if (!someParameterIsUnknown(parameterTypes)) {
            // give up and return all remaining matches
            return mostSpecificFunctions;
        }

        // look for functions that only cast the unknown arguments
        List<ApplicableFunction> unknownOnlyCastFunctions = getUnknownOnlyCastFunctions(applicableFunctions, parameterTypes);
        if (!unknownOnlyCastFunctions.isEmpty()) {
            mostSpecificFunctions = unknownOnlyCastFunctions;
            if (mostSpecificFunctions.size() == 1) {
                return mostSpecificFunctions;
            }
        }

        // If the return type for all the selected function is the same, and the parameters are declared as RETURN_NULL_ON_NULL
        // all the functions are semantically the same. We can return just any of those.
        if (returnTypeIsTheSame(mostSpecificFunctions) && allReturnNullOnGivenInputTypes(mostSpecificFunctions, parameterTypes)) {
            // make it deterministic
            ApplicableFunction selectedFunction = Ordering.usingToString()
                    .reverse()
                    .sortedCopy(mostSpecificFunctions)
                    .get(0);
            return ImmutableList.of(selectedFunction);
        }

        return mostSpecificFunctions;
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> candidates)
    {
        List<ApplicableFunction> representatives = new ArrayList<>();

        for (ApplicableFunction current : candidates) {
            boolean found = false;
            for (int i = 0; i < representatives.size(); i++) {
                ApplicableFunction representative = representatives.get(i);
                if (isMoreSpecificThan(current, representative)) {
                    representatives.set(i, current);
                }
                if (isMoreSpecificThan(current, representative) || isMoreSpecificThan(representative, current)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                representatives.add(current);
            }
        }

        return representatives;
    }

    private static boolean someParameterIsUnknown(List<Type> parameters)
    {
        return parameters.stream().anyMatch(type -> type.equals(UNKNOWN));
    }

    private List<ApplicableFunction> getUnknownOnlyCastFunctions(List<ApplicableFunction> applicableFunction, List<Type> actualParameters)
    {
        return applicableFunction.stream()
                .filter((function) -> onlyCastsUnknown(function, actualParameters))
                .collect(toImmutableList());
    }

    private boolean onlyCastsUnknown(ApplicableFunction applicableFunction, List<Type> actualParameters)
    {
        List<Type> boundTypes = resolveTypes(applicableFunction.getBoundSignature().getArgumentTypes(), typeManager);
        checkState(actualParameters.size() == boundTypes.size(), "type lists are of different lengths");
        for (int i = 0; i < actualParameters.size(); i++) {
            if (!boundTypes.get(i).equals(actualParameters.get(i)) && actualParameters.get(i) != UNKNOWN) {
                return false;
            }
        }
        return true;
    }

    private boolean returnTypeIsTheSame(List<ApplicableFunction> applicableFunctions)
    {
        Set<Type> returnTypes = applicableFunctions.stream()
                .map(function -> typeManager.getType(function.getBoundSignature().getReturnType()))
                .collect(Collectors.toSet());
        return returnTypes.size() == 1;
    }

    private boolean allReturnNullOnGivenInputTypes(List<ApplicableFunction> applicableFunctions, List<Type> parameters)
    {
        return applicableFunctions.stream().allMatch(x -> returnsNullOnGivenInputTypes(x, parameters));
    }

    private boolean returnsNullOnGivenInputTypes(ApplicableFunction applicableFunction, List<Type> parameterTypes)
    {
        Signature boundSignature = applicableFunction.getBoundSignature();
        FunctionKind functionKind = boundSignature.getKind();
        // Window and Aggregation functions have fixed semantic where NULL values are always skipped
        if (functionKind != SCALAR) {
            return true;
        }

        for (int i = 0; i < parameterTypes.size(); i++) {
            Type parameterType = parameterTypes.get(i);
            if (parameterType.equals(UNKNOWN)) {
                // The original implementation checks only whether the particular argument has @SqlNullable.
                // However, RETURNS NULL ON NULL INPUT / CALLED ON NULL INPUT is a function level metadata according
                // to SQL spec. So there is a loss of precision here.
                if (applicableFunction.isCalledOnNullInput()) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Optional<List<Type>> toTypes(List<TypeSignatureProvider> typeSignatureProviders, TypeManager typeManager)
    {
        ImmutableList.Builder<Type> resultBuilder = ImmutableList.builder();
        for (TypeSignatureProvider typeSignatureProvider : typeSignatureProviders) {
            if (typeSignatureProvider.hasDependency()) {
                return Optional.empty();
            }
            resultBuilder.add(typeManager.getType(typeSignatureProvider.getTypeSignature()));
        }
        return Optional.of(resultBuilder.build());
    }

    /**
     * One method is more specific than another if invocation handled by the first method could be passed on to the other one
     */
    private boolean isMoreSpecificThan(ApplicableFunction left, ApplicableFunction right)
    {
        List<TypeSignatureProvider> resolvedTypes = fromTypeSignatures(left.getBoundSignature().getArgumentTypes());
        Optional<BoundVariables> boundVariables = new SignatureBinder(typeManager, right.getDeclaredSignature(), true)
                .bindVariables(resolvedTypes);
        return boundVariables.isPresent();
    }

    private static class ApplicableFunction
    {
        private final Signature declaredSignature;
        private final Signature boundSignature;
        private final boolean calledOnNullInput;

        private ApplicableFunction(Signature declaredSignature, Signature boundSignature, boolean calledOnNullInput)
        {
            this.declaredSignature = declaredSignature;
            this.boundSignature = boundSignature;
            this.calledOnNullInput = calledOnNullInput;
        }

        public Signature getDeclaredSignature()
        {
            return declaredSignature;
        }

        public Signature getBoundSignature()
        {
            return boundSignature;
        }

        public boolean isCalledOnNullInput()
        {
            return calledOnNullInput;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("declaredSignature", declaredSignature)
                    .add("boundSignature", boundSignature)
                    .add("calledOnNullInput", calledOnNullInput)
                    .toString();
        }
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
