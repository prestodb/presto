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
package com.facebook.presto.sidecar.functionNamespace;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.LongVariableConstraint;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlFunctionSupplier;
import com.facebook.presto.spi.function.SqlInvokedAggregationFunctionImplementation;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.TypeSignatureUtils.resolveIntermediateType;
import static com.facebook.presto.spi.StandardErrorCode.DUPLICATE_FUNCTION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.CPP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class NativeFunctionNamespaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private static final Logger log = Logger.get(NativeFunctionNamespaceManager.class);
    private final Map<QualifiedObjectName, UserDefinedType> userDefinedTypes = new ConcurrentHashMap<>();
    private final Map<SqlFunctionHandle, AggregationFunctionImplementation> aggregationImplementationByHandle = new ConcurrentHashMap<>();
    private final FunctionDefinitionProvider functionDefinitionProvider;
    private final NodeManager nodeManager;
    private final Map<SqlFunctionId, SqlInvokedFunction> functions = new ConcurrentHashMap<>();
    private final Supplier<Map<SqlFunctionId, SqlInvokedFunction>> memoizedFunctionsSupplier;
    private final FunctionMetadataManager functionMetadataManager;
    private final LoadingCache<Signature, SqlFunctionSupplier> specializedFunctionKeyCache;

    @Inject
    public NativeFunctionNamespaceManager(
            @ServingCatalog String catalogName,
            SqlFunctionExecutors sqlFunctionExecutors,
            SqlInvokedFunctionNamespaceManagerConfig config,
            FunctionDefinitionProvider functionDefinitionProvider,
            NodeManager nodeManager,
            FunctionMetadataManager functionMetadataManager)
    {
        super(catalogName, sqlFunctionExecutors, config);
        this.functionDefinitionProvider = requireNonNull(functionDefinitionProvider, "functionDefinitionProvider is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.memoizedFunctionsSupplier = Suppliers.memoizeWithExpiration(this::bootstrapNamespace,
                config.getFunctionCacheExpiration().toMillis(), TimeUnit.MILLISECONDS);
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.specializedFunctionKeyCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::doGetSpecializedFunctionKey));
    }

    private SqlFunctionSupplier doGetSpecializedFunctionKey(Signature signature)
    {
        return functionMetadataManager.getSpecializedFunctionKey(signature);
    }

    private synchronized Map<SqlFunctionId, SqlInvokedFunction> bootstrapNamespace()
    {
        functions.clear();
        UdfFunctionSignatureMap nativeFunctionSignatureMap = functionDefinitionProvider.getUdfDefinition(nodeManager);
        if (nativeFunctionSignatureMap == null || nativeFunctionSignatureMap.isEmpty()) {
            return ImmutableMap.of();
        }
        populateNamespaceManager(nativeFunctionSignatureMap);
        checkArgument(!functions.isEmpty(), "functions map is empty !");
        return unmodifiableMap(functions);
    }

    private synchronized void populateNamespaceManager(UdfFunctionSignatureMap udfFunctionSignatureMap)
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap = udfFunctionSignatureMap.getUDFSignatureMap();
        udfSignatureMap.forEach((name, metaInfoList) -> {
            List<SqlInvokedFunction> functions = metaInfoList.stream().map(metaInfo -> createSqlInvokedFunction(name, metaInfo)).collect(toImmutableList());
            functions.forEach(function -> createFunction(function, false));
        });
    }

    @Override
    public final AggregationFunctionImplementation getAggregateFunctionImplementation(FunctionHandle functionHandle, TypeManager typeManager)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());

        SqlFunctionHandle sqlFunctionHandle = (SqlFunctionHandle) functionHandle;
        if (aggregationImplementationByHandle.containsKey(sqlFunctionHandle)) {
            return aggregationImplementationByHandle.get(sqlFunctionHandle);
        }
        if (functionHandle instanceof NativeFunctionHandle) {
            return processNativeFunctionHandle((NativeFunctionHandle) sqlFunctionHandle, typeManager);
        }
        else {
            return processSqlFunctionHandle(sqlFunctionHandle, typeManager);
        }
    }

    private AggregationFunctionImplementation processNativeFunctionHandle(NativeFunctionHandle nativeFunctionHandle, TypeManager typeManager)
    {
        Signature signature = nativeFunctionHandle.getSignature();
        SqlFunction function = getSqlFunctionFromSignature(signature);
        SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;

        checkArgument(
                sqlFunction.getAggregationMetadata().isPresent(),
                "Need aggregationMetadata to get aggregation function implementation");

        AggregationFunctionMetadata aggregationMetadata = sqlFunction.getAggregationMetadata().get();
        TypeSignature intermediateType = aggregationMetadata.getIntermediateType();
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                intermediateType, sqlFunction.getFunctionId().getArgumentTypes(), signature.getArgumentTypes());
        List<Type> parameters = signature.getArgumentTypes().stream().map(
                (typeManager::getType)).collect(toImmutableList());
        aggregationImplementationByHandle.put(
                nativeFunctionHandle,
                new SqlInvokedAggregationFunctionImplementation(
                        typeManager.getType(resolvedIntermediateType),
                        typeManager.getType(signature.getReturnType()),
                        aggregationMetadata.isOrderSensitive(),
                        parameters));
        return aggregationImplementationByHandle.get(nativeFunctionHandle);
    }

    private AggregationFunctionImplementation processSqlFunctionHandle(SqlFunctionHandle sqlFunctionHandle, TypeManager typeManager)
    {
        SqlFunctionId functionId = sqlFunctionHandle.getFunctionId();
        if (!memoizedFunctionsSupplier.get().containsKey(functionId)) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Function '%s' is missing from cache", functionId.getId()));
        }

        aggregationImplementationByHandle.put(
                sqlFunctionHandle,
                sqlInvokedFunctionToAggregationImplementation(memoizedFunctionsSupplier.get().get(functionId), typeManager));
        return aggregationImplementationByHandle.get(sqlFunctionHandle);
    }

    protected synchronized SqlInvokedFunction createSqlInvokedFunction(String functionName, JsonBasedUdfFunctionMetadata jsonBasedUdfFunctionMetaData)
    {
        checkState(jsonBasedUdfFunctionMetaData.getRoutineCharacteristics().getLanguage().equals(CPP), "NativeFunctionNamespaceManager only supports CPP UDF");
        QualifiedObjectName qualifiedFunctionName = QualifiedObjectName.valueOf(new CatalogSchemaName(getCatalogName(), jsonBasedUdfFunctionMetaData.getSchema()), functionName);
        List<String> parameterNameList = jsonBasedUdfFunctionMetaData.getParamNames();
        List<TypeSignature> parameterTypeList = convertApplicableTypeToVariable(jsonBasedUdfFunctionMetaData.getParamTypes());
        List<TypeVariableConstraint> typeVariableConstraintsList = jsonBasedUdfFunctionMetaData.getTypeVariableConstraints().isPresent() ?
                jsonBasedUdfFunctionMetaData.getTypeVariableConstraints().get() : Collections.emptyList();
        List<LongVariableConstraint> longVariableConstraintList = jsonBasedUdfFunctionMetaData.getLongVariableConstraints().isPresent() ?
                jsonBasedUdfFunctionMetaData.getLongVariableConstraints().get() : Collections.emptyList();

        TypeSignature outputType = convertApplicableTypeToVariable(jsonBasedUdfFunctionMetaData.getOutputType());
        ImmutableList.Builder<Parameter> parameterBuilder = ImmutableList.builder();
        for (int i = 0; i < parameterNameList.size(); i++) {
            parameterBuilder.add(new Parameter(parameterNameList.get(i), parameterTypeList.get(i)));
        }

        Optional<AggregationFunctionMetadata> aggregationFunctionMetadata =
                jsonBasedUdfFunctionMetaData.getAggregateMetadata()
                        .map(metadata -> new AggregationFunctionMetadata(
                                convertApplicableTypeToVariable(metadata.getIntermediateType()),
                                metadata.isOrderSensitive()));

        return new SqlInvokedFunction(
                qualifiedFunctionName,
                parameterBuilder.build(),
                typeVariableConstraintsList,
                longVariableConstraintList,
                outputType,
                jsonBasedUdfFunctionMetaData.getDocString(),
                jsonBasedUdfFunctionMetaData.getRoutineCharacteristics(),
                "",
                jsonBasedUdfFunctionMetaData.getVariableArity(),
                notVersioned(),
                jsonBasedUdfFunctionMetaData.getFunctionKind(),
                aggregationFunctionMetadata);
    }

    @Override
    protected Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName)
    {
        return memoizedFunctionsSupplier.get().values().stream()
                .filter(function -> function.getSignature().getName().equals(functionName))
                .collect(toImmutableList());
    }

    @Override
    protected UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName)
    {
        return userDefinedTypes.get(typeName);
    }

    @Override
    protected FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
    {
        if (functionHandle instanceof NativeFunctionHandle) {
            return getMetadataFromNativeFunctionHandle(functionHandle);
        }

        return fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(this::sqlInvokedFunctionToMetadata).collect(onlyElement());
    }

    @Override
    protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
    {
        return fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(this::sqlInvokedFunctionToImplementation)
                .collect(onlyElement());
    }

    @Override
    public synchronized void createFunction(SqlInvokedFunction function, boolean replace)
    {
        checkFunctionLanguageSupported(function);
        SqlFunctionId functionId = function.getFunctionId();
        if (!replace && functions.containsKey(function.getFunctionId())) {
            throw new PrestoException(DUPLICATE_FUNCTION_ERROR, format("Function '%s' already exists", functionId.getId()));
        }

        SqlInvokedFunction replacedFunction = functions.get(functionId);
        long version = 1;
        if (replacedFunction != null) {
            version = parseLong(replacedFunction.getRequiredVersion()) + 1;
        }
        functions.put(functionId, function.withVersion(String.valueOf(version)));
    }

    @Override
    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new PrestoException(NOT_SUPPORTED, "Alter Function is not supported in NativeFunctionNamespaceManager");
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(NOT_SUPPORTED, "Drop Function is not supported in NativeFunctionNamespaceManager");
    }

    @Override
    public Collection<SqlInvokedFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return memoizedFunctionsSupplier.get().values();
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        QualifiedObjectName name = userDefinedType.getUserDefinedTypeName();
        checkArgument(
                !userDefinedTypes.containsKey(name),
                "Parametric type %s already registered",
                name);
        userDefinedTypes.put(name, userDefinedType);
    }

    @Override
    public final FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        FunctionHandle functionHandle = super.getFunctionHandle(transactionHandle, signature);

        // only handle generic variadic signatures here , for normal signature we use the AbstractSqlInvokedFunctionNamespaceManager function handle.
        if (functionHandle == null) {
            return new NativeFunctionHandle(signature);
        }
        return functionHandle;
    }

    // Todo: Improve the handling of parameter type differentiation in native execution.
    // HACK: Currently, we lack support for correctly identifying the parameterKind, specifically between TYPE and VARIABLE,
    // in native execution. The following utility functions help bridge this gap by parsing the type signature and verifying whether its base
    // and parameters are of a supported type. The valid types list are non - parametric types that Presto supports.

    public static TypeSignature convertApplicableTypeToVariable(TypeSignature typeSignature)
    {
        List<TypeSignature> typeSignaturesList = convertApplicableTypeToVariable(ImmutableList.of(typeSignature));
        checkArgument(!typeSignaturesList.isEmpty(), "Type signature list is empty for : " + typeSignature);
        return typeSignaturesList.get(0);
    }

    public static List<TypeSignature> convertApplicableTypeToVariable(List<TypeSignature> typeSignatures)
    {
        List<TypeSignature> newTypeSignaturesList = new ArrayList<>();
        for (TypeSignature typeSignature : typeSignatures) {
            if (!typeSignature.getParameters().isEmpty()) {
                TypeSignature newTypeSignature =
                        new TypeSignature(
                                typeSignature.getBase(),
                                getTypeSignatureParameters(
                                        typeSignature,
                                        typeSignature.getParameters()));
                newTypeSignaturesList.add(newTypeSignature);
            }
            else {
                newTypeSignaturesList.add(typeSignature);
            }
        }
        return newTypeSignaturesList;
    }

    private static List<TypeSignatureParameter> getTypeSignatureParameters(
            TypeSignature typeSignature,
            List<TypeSignatureParameter> typeSignatureParameterList)
    {
        List<TypeSignatureParameter> newParameterTypeList = new ArrayList<>();
        for (TypeSignatureParameter parameter : typeSignatureParameterList) {
            if (parameter.isLongLiteral()) {
                newParameterTypeList.add(parameter);
                continue;
            }

            boolean isNamedTypeSignature = parameter.isNamedTypeSignature();
            TypeSignature parameterTypeSignature;
            // If it's a named type signatures only in the case of row signature types.
            if (isNamedTypeSignature) {
                parameterTypeSignature = parameter.getNamedTypeSignature().getTypeSignature();
            }
            else {
                parameterTypeSignature = parameter.getTypeSignature();
            }

            if (parameterTypeSignature.getParameters().isEmpty()) {
                boolean changeTypeToVariable = isDecimalTypeBase(typeSignature.getBase());
                if (changeTypeToVariable) {
                    newParameterTypeList.add(
                            TypeSignatureParameter.of(parameterTypeSignature.getBase()));
                }
                else {
                    if (isNamedTypeSignature) {
                        newParameterTypeList.add(TypeSignatureParameter.of(parameter.getNamedTypeSignature()));
                    }
                    else {
                        newParameterTypeList.add(TypeSignatureParameter.of(parameterTypeSignature));
                    }
                }
            }
            else {
                TypeSignature newTypeSignature =
                        new TypeSignature(
                                parameterTypeSignature.getBase(),
                                getTypeSignatureParameters(
                                        parameterTypeSignature.getStandardTypeSignature(),
                                        parameterTypeSignature.getParameters()));
                if (isNamedTypeSignature) {
                    newParameterTypeList.add(
                            TypeSignatureParameter.of(
                                    new NamedTypeSignature(
                                            Optional.empty(),
                                            newTypeSignature)));
                }
                else {
                    newParameterTypeList.add(TypeSignatureParameter.of(newTypeSignature));
                }
            }
        }
        return newParameterTypeList;
    }

    private static boolean isDecimalTypeBase(String typeBase)
    {
        return typeBase.equals(StandardTypes.DECIMAL);
    }

    // Hack ends here

    private SqlFunction getSqlFunctionFromSignature(Signature signature)
    {
        try {
            return specializedFunctionKeyCache.getUnchecked(signature).getFunction();
        }
        catch (UncheckedExecutionException e) {
            throw convertToPrestoException(e, format("Error getting FunctionMetadata for signature: %s", signature));
        }
    }

    private FunctionMetadata getMetadataFromNativeFunctionHandle(SqlFunctionHandle functionHandle)
    {
        NativeFunctionHandle nativeFunctionHandle = (NativeFunctionHandle) functionHandle;
        Signature signature = nativeFunctionHandle.getSignature();
        SqlFunction function = getSqlFunctionFromSignature(signature);

        SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
        return new FunctionMetadata(
                signature.getName(),
                signature.getArgumentTypes(),
                sqlFunction.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(toImmutableList()),
                signature.getReturnType(),
                function.getSignature().getKind(),
                sqlFunction.getRoutineCharacteristics().getLanguage(),
                getFunctionImplementationType(sqlFunction),
                function.isDeterministic(),
                function.isCalledOnNullInput(),
                sqlFunction.getVersion(),
                function.getComplexTypeFunctionDescriptor());
    }

    private static PrestoException convertToPrestoException(UncheckedExecutionException exception, String failureMessage)
    {
        Throwable cause = exception.getCause();
        if (cause instanceof PrestoException) {
            return (PrestoException) cause;
        }
        return new PrestoException(GENERIC_INTERNAL_ERROR, failureMessage, cause);
    }
}
