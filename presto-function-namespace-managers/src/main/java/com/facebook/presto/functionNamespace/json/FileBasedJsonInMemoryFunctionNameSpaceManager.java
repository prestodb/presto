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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.plugin.base.JsonUtils.parseJson;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

public class FileBasedJsonInMemoryFunctionNameSpaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private final Map<SqlFunctionId, SqlInvokedFunction> latestFunctions = new ConcurrentHashMap<>();
    private final Map<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final Map<QualifiedObjectName, UserDefinedType> userDefinedTypes = new ConcurrentHashMap<>();
    private final FileBasedJsonConfig fileconfig;

    @Inject
    public FileBasedJsonInMemoryFunctionNameSpaceManager(
            @ServingCatalog String catalogName,
            SqlFunctionExecutors sqlFunctionExecutors,
            SqlInvokedFunctionNamespaceManagerConfig config,
            FileBasedJsonConfig fileConfig)
    {
        super(catalogName, sqlFunctionExecutors, config);
        this.fileconfig = fileConfig;
        bootStrapNameSpaceFromFile();
    }

    private void bootStrapNameSpaceFromFile()
    {
        FunctionUDFSignatureMap functionUDFSignatureMap = parseJson(Paths.get(fileconfig.getFunctionDefinitionFile()), FunctionUDFSignatureMap.class);
        if (functionUDFSignatureMap.isEmpty()) {
            return;
        }
        populateNameSpaceManager(functionUDFSignatureMap);
    }

    private void populateNameSpaceManager(FunctionUDFSignatureMap functionUDFSignatureMap)
    {
        Map<String, List<FunctionUDFMetaInfo>> udfSignatureMap = functionUDFSignatureMap.getUDFSignatureMap();
        Iterator<Map.Entry<String, List<FunctionUDFMetaInfo>>> iterator = udfSignatureMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<FunctionUDFMetaInfo>> entry = iterator.next();
            String functionName = entry.getKey();
            for (int i = 0; i < entry.getValue().size(); i++) {
                SqlInvokedFunction sqlInvokedFunction = createSqlInvokedFunction(functionName, entry.getValue().get(i));
                createFunction(sqlInvokedFunction, false);
            }
        }
    }

    private SqlInvokedFunction createSqlInvokedFunction(String functionName, FunctionUDFMetaInfo functionUDFMetaInfo)
    {
        // todo: need to remove hardcoded schema name
        QualifiedObjectName qualifiedFunctionName = QualifiedObjectName.valueOf(new CatalogSchemaName(getCatalogName(), "f3"), functionName);
        List<String> parameterNameList = functionUDFMetaInfo.getParamNames();
        List<String> parameterTypeList = functionUDFMetaInfo.getParamTypes();
        if (parameterNameList.size() != parameterTypeList.size()) {
            throw new PrestoException(INVALID_ARGUMENTS,
                    format("Function %s provided with mismatch in number of parameter names (%d) and " +
                            "parameter types (%d) ", qualifiedFunctionName, parameterNameList.size(),
                            parameterTypeList.size()));
        }

        ImmutableList.Builder<Parameter> parameterBuilder = ImmutableList.builder();
        for (int i = 0; i < parameterNameList.size(); i++) {
            parameterBuilder.add(new Parameter(parameterNameList.get(i), parseTypeSignature(parameterTypeList.get(i))));
        }

        return new SqlInvokedFunction(
                qualifiedFunctionName,
                parameterBuilder.build(),
                parseTypeSignature(functionUDFMetaInfo.getOutputTypes()),
                functionUDFMetaInfo.getDocString(),
                RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).setLanguage(new RoutineCharacteristics.Language("cpp")).build(),
                "External",
                notVersioned());
    }

    @Override
    protected Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName)
    {
        return latestFunctions.values().stream()
                .filter(function -> function.getSignature().getName().equals(functionName))
                .map(FileBasedJsonInMemoryFunctionNameSpaceManager::copyFunction)
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
        throw new PrestoException(NOT_SUPPORTED, "Drop Function is not supported in FileBasedJsonInMemoryFunctionNameSpaceManager");
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(NOT_SUPPORTED, "Drop Function is not supported in FileBasedJsonInMemoryFunctionNameSpaceManager");
    }

    @Override
    public Collection<SqlInvokedFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return latestFunctions.values();
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        //Need to add a test
        QualifiedObjectName name = userDefinedType.getUserDefinedTypeName();
        checkArgument(
                !userDefinedTypes.containsKey(name),
                "Parametric type %s already registered",
                name);
        userDefinedTypes.put(name, userDefinedType);
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
                function.getVersion());
    }
}
