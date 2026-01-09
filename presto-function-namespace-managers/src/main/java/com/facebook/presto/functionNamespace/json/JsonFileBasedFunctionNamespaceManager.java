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
package com.facebook.presto.functionNamespace.json;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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
import static java.util.Objects.requireNonNull;

public class JsonFileBasedFunctionNamespaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private static final Logger log = Logger.get(JsonFileBasedFunctionNamespaceManager.class);

    private final Map<SqlFunctionId, SqlInvokedFunction> latestFunctions = new ConcurrentHashMap<>();
    private final Map<QualifiedObjectName, UserDefinedType> userDefinedTypes = new ConcurrentHashMap<>();
    private final JsonFileBasedFunctionNamespaceManagerConfig managerConfig;
    private final FunctionDefinitionProvider functionDefinitionProvider;
    private final Map<SqlFunctionHandle, AggregationFunctionImplementation> aggregationImplementationByHandle = new ConcurrentHashMap<>();

    @Inject
    public JsonFileBasedFunctionNamespaceManager(
            @ServingCatalog String catalogName,
            SqlFunctionExecutors sqlFunctionExecutors,
            SqlInvokedFunctionNamespaceManagerConfig config,
            JsonFileBasedFunctionNamespaceManagerConfig managerConfig,
            FunctionDefinitionProvider functionDefinitionProvider)
    {
        super(catalogName, sqlFunctionExecutors, config);
        this.managerConfig = requireNonNull(managerConfig, "managerConfig is null");
        this.functionDefinitionProvider = requireNonNull(functionDefinitionProvider, "functionDefinitionProvider is null");
        bootstrapNamespaceFromFile();
    }

    @Override
    public final AggregationFunctionImplementation getAggregateFunctionImplementation(FunctionHandle functionHandle, TypeManager typeManager)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());

        SqlFunctionHandle sqlFunctionHandle = (SqlFunctionHandle) functionHandle;

        // Cache results if applicable
        if (!aggregationImplementationByHandle.containsKey(sqlFunctionHandle)) {
            SqlFunctionId functionId = sqlFunctionHandle.getFunctionId();
            if (!latestFunctions.containsKey(functionId)) {
                throw new PrestoException(GENERIC_USER_ERROR, format("Function '%s' is missing from cache", functionId.getId()));
            }

            aggregationImplementationByHandle.put(
                    sqlFunctionHandle,
                    sqlInvokedFunctionToAggregationImplementation(latestFunctions.get(functionId), typeManager));
        }

        return aggregationImplementationByHandle.get(sqlFunctionHandle);
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
                function.getVersion(),
                function.getSignature().getKind(),
                function.getAggregationMetadata());
    }

    private void bootstrapNamespaceFromFile()
    {
        UdfFunctionSignatureMap udfFunctionSignatureMap = functionDefinitionProvider.getUdfDefinition(managerConfig.getFunctionDefinitionPath());
        if (udfFunctionSignatureMap == null || udfFunctionSignatureMap.isEmpty()) {
            return;
        }
        populateNameSpaceManager(udfFunctionSignatureMap);
    }

    private void populateNameSpaceManager(UdfFunctionSignatureMap udfFunctionSignatureMap)
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap = udfFunctionSignatureMap.getUDFSignatureMap();
        udfSignatureMap.forEach((name, metaInfoList) -> {
            List<SqlInvokedFunction> functions = metaInfoList.stream().map(metaInfo -> createSqlInvokedFunction(name, metaInfo)).collect(toImmutableList());
            functions.forEach(function -> createFunction(function, false));
        });
    }

    private SqlInvokedFunction createSqlInvokedFunction(String functionName, JsonBasedUdfFunctionMetadata jsonBasedUdfFunctionMetaData)
    {
        checkState(jsonBasedUdfFunctionMetaData.getRoutineCharacteristics().getLanguage().equals(CPP), "JsonFileBasedFunctionNamespaceManager only supports CPP UDF");
        QualifiedObjectName qualifiedFunctionName = QualifiedObjectName.valueOf(new CatalogSchemaName(getCatalogName(), jsonBasedUdfFunctionMetaData.getSchema()), functionName);
        List<String> parameterNameList = jsonBasedUdfFunctionMetaData.getParamNames();
        List<TypeSignature> parameterTypeList = jsonBasedUdfFunctionMetaData.getParamTypes();

        ImmutableList.Builder<Parameter> parameterBuilder = ImmutableList.builder();
        for (int i = 0; i < parameterNameList.size(); i++) {
            parameterBuilder.add(new Parameter(parameterNameList.get(i), parameterTypeList.get(i)));
        }

        return new SqlInvokedFunction(
                qualifiedFunctionName,
                parameterBuilder.build(),
                jsonBasedUdfFunctionMetaData.getOutputType(),
                jsonBasedUdfFunctionMetaData.getDocString(),
                jsonBasedUdfFunctionMetaData.getRoutineCharacteristics(),
                "",
                notVersioned(),
                jsonBasedUdfFunctionMetaData.getFunctionKind(),
                jsonBasedUdfFunctionMetaData.getAggregateMetadata());
    }

    @Override
    protected Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName)
    {
        return latestFunctions.values().stream()
                .filter(function -> function.getSignature().getName().equals(functionName))
                .map(JsonFileBasedFunctionNamespaceManager::copyFunction)
                .collect(toImmutableList());
    }

    @Override
    protected FunctionMetadata sqlInvokedFunctionToMetadata(SqlInvokedFunction function)
    {
        return new FunctionMetadata(
                function.getSignature().getName(),
                function.getSignature().getArgumentTypes(),
                function.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(toImmutableList()),
                function.getSignature().getReturnType(),
                function.getSignature().getKind(),
                function.getRoutineCharacteristics().getLanguage(),
                getFunctionImplementationType(function),
                function.isDeterministic(),
                function.isCalledOnNullInput(),
                function.getVersion(),
                function.getDescription());
    }

    @Override
    protected UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName)
    {
        return userDefinedTypes.get(typeName);
    }

    @Override
    protected FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
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

    @Override
    public void createFunction(SqlInvokedFunction function, boolean replace)
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
        throw new PrestoException(NOT_SUPPORTED, "Alter Function is not supported in JsonFileBasedFunctionNamespaceManager");
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(NOT_SUPPORTED, "Drop Function is not supported in JsonFileBasedFunctionNamespaceManager");
    }

    @Override
    public Collection<SqlInvokedFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return latestFunctions.values();
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
}
