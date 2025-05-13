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
package com.facebook.presto.functionNamespace.rest;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.InvalidFunctionHandleException;
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
import com.facebook.presto.spi.function.FunctionVersion;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RestFunctionHandle;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class RestBasedFunctionNamespaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private static final Logger log = Logger.get(RestBasedFunctionNamespaceManager.class);
    private final RestBasedFunctionApis restApis;
    private final List<SqlInvokedFunction> latestFunctions = new ArrayList<>();
    private final AtomicReference<Optional<String>> cachedETag = new AtomicReference<>(Optional.empty());

    @Inject
    public RestBasedFunctionNamespaceManager(
            @ServingCatalog String catalogName,
            SqlFunctionExecutors sqlFunctionExecutors,
            SqlInvokedFunctionNamespaceManagerConfig config,
            RestBasedFunctionApis restApis)
    {
        super(catalogName, sqlFunctionExecutors, config);
        this.restApis = requireNonNull(restApis, "restApis is null");
    }

    @Override
    public final AggregationFunctionImplementation getAggregateFunctionImplementation(FunctionHandle functionHandle, TypeManager typeManager)
    {
        throw new PrestoException(NOT_SUPPORTED, "Aggregate Function is not supported in RestBasedFunctionNamespaceManager");
    }

    private List<SqlInvokedFunction> getLatestFunctions()
    {
        // Check if the function list has been modified.
        String newETag = restApis.getFunctionsETag();
        Optional<String> currentETag = cachedETag.get();
        if (newETag != null && currentETag.isPresent() && cachedETag.get().equals(newETag)) {
            return latestFunctions;
        }

        // Clear cached list of functions and get the latest list.
        latestFunctions.clear();
        UdfFunctionSignatureMap udfFunctionSignatureMap = restApis.getAllFunctions();
        if (udfFunctionSignatureMap == null || udfFunctionSignatureMap.isEmpty()) {
            return ImmutableList.of();
        }

        List<SqlInvokedFunction> newFunctions = createSqlInvokedFunctions(udfFunctionSignatureMap);
        latestFunctions.addAll(newFunctions);

        if (newETag != null) {
            cachedETag.set(Optional.of(newETag));
        }

        return latestFunctions;
    }

    private List<SqlInvokedFunction> createSqlInvokedFunctions(UdfFunctionSignatureMap udfFunctionSignatureMap)
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap = udfFunctionSignatureMap.getUDFSignatureMap();
        return udfSignatureMap.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(metaInfo -> createSqlInvokedFunction(entry.getKey(), metaInfo)))
                .collect(toImmutableList());
    }

    private SqlInvokedFunction createSqlInvokedFunction(String functionName, JsonBasedUdfFunctionMetadata jsonBasedUdfFunctionMetaData)
    {
        QualifiedObjectName qualifiedFunctionName = QualifiedObjectName.valueOf(new CatalogSchemaName(getCatalogName(), jsonBasedUdfFunctionMetaData.getSchema()), functionName);
        List<String> parameterNameList = jsonBasedUdfFunctionMetaData.getParamNames();
        List<TypeSignature> parameterTypeList = jsonBasedUdfFunctionMetaData.getParamTypes();

        ImmutableList.Builder<Parameter> parameterBuilder = ImmutableList.builder();
        for (int i = 0; i < parameterNameList.size(); i++) {
            parameterBuilder.add(new Parameter(parameterNameList.get(i), parameterTypeList.get(i)));
        }

        FunctionVersion functionVersion = new FunctionVersion(jsonBasedUdfFunctionMetaData.getVersion());
        SqlFunctionId functionId = jsonBasedUdfFunctionMetaData.getFunctionId().orElse(null);
        // The function server may return a functionId with a different catalog name.  We need to update the catalog name to the current catalog name.
        functionId = new SqlFunctionId(
                new QualifiedObjectName(
                        getCatalogName(),
                        functionId.getFunctionName().getSchemaName(),
                        functionId.getFunctionName().getObjectName()),
                functionId.getArgumentTypes());
        return new SqlInvokedFunction(
                qualifiedFunctionName,
                parameterBuilder.build(),
                emptyList(),
                emptyList(),
                jsonBasedUdfFunctionMetaData.getOutputType(),
                jsonBasedUdfFunctionMetaData.getDocString(),
                jsonBasedUdfFunctionMetaData.getRoutineCharacteristics(),
                "",
                jsonBasedUdfFunctionMetaData.getVariableArity(),
                functionVersion,
                jsonBasedUdfFunctionMetaData.getFunctionKind(),
                functionId,
                jsonBasedUdfFunctionMetaData.getAggregateMetadata(),
                Optional.of(new RestFunctionHandle(
                        functionId,
                        functionVersion.toString(),
                        new Signature(
                                qualifiedFunctionName,
                                jsonBasedUdfFunctionMetaData.getFunctionKind(),
                                jsonBasedUdfFunctionMetaData.getOutputType(),
                                jsonBasedUdfFunctionMetaData.getParamTypes()))));
    }

    @Override
    protected Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName)
    {
        UdfFunctionSignatureMap udfFunctionSignatureMap = restApis.getFunctions(functionName.getSchemaName(), functionName.getObjectName());
        if (udfFunctionSignatureMap == null || udfFunctionSignatureMap.isEmpty()) {
            return ImmutableList.of();
        }

        List<SqlInvokedFunction> functions = createSqlInvokedFunctions(udfFunctionSignatureMap);
        return functions;
    }

    @Override
    protected UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName)
    {
        throw new PrestoException(NOT_SUPPORTED, "User Defined Type is not supported in RestBasedFunctionNamespaceManager");
    }

    protected Optional<SqlInvokedFunction> getSqlInvokedFunction(SqlFunctionHandle functionHandle)
    {
        Collection<SqlInvokedFunction> functions = fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName());

        return functions.stream()
                .filter(sqlFunction -> sqlFunction.getFunctionId().equals(functionHandle.getFunctionId()) &&
                        sqlFunction.getVersion().toString().equals(functionHandle.getVersion()))
                .findFirst();
    }

    @Override
    protected FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);

        Optional<SqlInvokedFunction> function = getSqlInvokedFunction(functionHandle);
        if (!function.isPresent()) {
            throw new InvalidFunctionHandleException(functionHandle);
        }

        return sqlInvokedFunctionToMetadata(function.get());
    }

    @Override
    protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);

        Optional<SqlInvokedFunction> function = getSqlInvokedFunction(functionHandle);
        if (!function.isPresent()) {
            throw new InvalidFunctionHandleException(functionHandle);
        }

        return sqlInvokedFunctionToImplementation(function.get());
    }

    @Override
    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "Create Function is not supported in RestBasedFunctionNamespaceManager");
    }

    @Override
    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new PrestoException(NOT_SUPPORTED, "Alter Function is not supported in RestBasedFunctionNamespaceManager");
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(NOT_SUPPORTED, "Drop Function is not supported in RestBasedFunctionNamespaceManager");
    }

    @Override
    public Collection<SqlInvokedFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return getLatestFunctions();
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        throw new PrestoException(NOT_SUPPORTED, "Add User Defined Type is not supported in RestBasedFunctionNamespaceManager");
    }
}
