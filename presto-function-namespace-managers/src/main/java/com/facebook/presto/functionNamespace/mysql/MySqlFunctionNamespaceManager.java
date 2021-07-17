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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.InvalidFunctionHandleException;
import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import org.jdbi.v3.core.Jdbi;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.hash.Hashing.sha256;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class MySqlFunctionNamespaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private static final int MAX_CATALOG_NAME_LENGTH = 128;
    private static final int MAX_SCHEMA_NAME_LENGTH = 128;
    private static final int MAX_FUNCTION_NAME_LENGTH = 256;
    private static final int MAX_PARAMETER_COUNT = 100;
    private static final int MAX_PARAMETER_NAME_LENGTH = 100;
    private static final int MAX_PARAMETER_TYPES_LENGTH = 30000;
    private static final int MAX_RETURN_TYPE_LENGTH = 30000;
    private final Jdbi jdbi;
    private final FunctionNamespaceDao functionNamespaceDao;
    private final Class<? extends FunctionNamespaceDao> functionNamespaceDaoClass;

    @Inject
    public MySqlFunctionNamespaceManager(
            Jdbi jdbi,
            FunctionNamespaceDao functionNamespaceDao,
            Class<? extends FunctionNamespaceDao> functionNamespaceDaoClass,
            SqlInvokedFunctionNamespaceManagerConfig managerConfig,
            SqlFunctionExecutors sqlFunctionExecutors,
            @ServingCatalog String catalogName)
    {
        super(catalogName, sqlFunctionExecutors, managerConfig);
        this.jdbi = requireNonNull(jdbi, "jdbi is null");
        this.functionNamespaceDao = requireNonNull(functionNamespaceDao, "functionNamespaceDao is null");
        this.functionNamespaceDaoClass = requireNonNull(functionNamespaceDaoClass, "functionNamespaceDaoClass is null");
    }

    @PostConstruct
    public void initialize()
    {
        functionNamespaceDao.createFunctionNamespacesTableIfNotExists();
        functionNamespaceDao.createSqlFunctionsTableIfNotExists();
        functionNamespaceDao.createUserDefinedTypesTableIfNotExists();
    }

    @Override
    public Collection<SqlInvokedFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return likePattern.map(pattern -> functionNamespaceDao.listFunctions(getCatalogName(), pattern, escape.orElse("\\"))).orElse(functionNamespaceDao.listFunctions(getCatalogName()));
    }

    @Override
    public void addUserDefinedType(UserDefinedType type)
    {
        jdbi.useTransaction(handle -> {
            FunctionNamespaceDao transactionDao = handle.attach(functionNamespaceDaoClass);
            QualifiedObjectName typeName = type.getUserDefinedTypeName();
            if (functionNamespaceDao.typeExists(typeName.getCatalogName(), typeName.getSchemaName(), typeName.getObjectName())) {
                throw new PrestoException(ALREADY_EXISTS, format("Type %s already exists", typeName));
            }
            transactionDao.insertUserDefinedType(typeName.getCatalogName(), typeName.getSchemaName(), typeName.getObjectName(), type.getPhysicalTypeSignature().toString());
        });
    }

    @Override
    public UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName)
    {
        Optional<UserDefinedType> type = functionNamespaceDao.getUserDefinedType(typeName.getCatalogName(), typeName.getSchemaName(), typeName.getObjectName());
        if (!type.isPresent()) {
            throw new PrestoException(NOT_FOUND, format("Type %s not found", typeName));
        }
        return type.get();
    }

    @Override
    protected Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName)
    {
        checkCatalog(functionName);
        return functionNamespaceDao.getFunctions(
                functionName.getCatalogName(),
                functionName.getSchemaName(),
                functionName.getObjectName());
    }

    @Override
    protected FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        Optional<SqlInvokedFunction> function = functionNamespaceDao.getFunction(hash(functionHandle.getFunctionId()), functionHandle.getFunctionId(), getLongVersion(functionHandle));
        if (!function.isPresent()) {
            throw new InvalidFunctionHandleException(functionHandle);
        }
        return sqlInvokedFunctionToMetadata(function.get());
    }

    @Override
    protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        Optional<SqlInvokedFunction> function = functionNamespaceDao.getFunction(hash(functionHandle.getFunctionId()), functionHandle.getFunctionId(), getLongVersion(functionHandle));
        if (!function.isPresent()) {
            throw new InvalidFunctionHandleException(functionHandle);
        }
        return sqlInvokedFunctionToImplementation(function.get());
    }

    @Override
    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        checkCatalog(function);
        checkFunctionLanguageSupported(function);
        checkArgument(!function.hasVersion(), "function '%s' is already versioned", function);

        QualifiedObjectName functionName = function.getFunctionId().getFunctionName();
        checkFieldLength("Catalog name", functionName.getCatalogName(), MAX_CATALOG_NAME_LENGTH);
        checkFieldLength("Schema name", functionName.getSchemaName(), MAX_SCHEMA_NAME_LENGTH);
        if (!functionNamespaceDao.functionNamespaceExists(functionName.getCatalogName(), functionName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, format("Function namespace not found: %s", functionName.getCatalogSchemaName()));
        }
        checkFieldLength("Function name", functionName.getObjectName(), MAX_FUNCTION_NAME_LENGTH);

        if (function.getParameters().size() > MAX_PARAMETER_COUNT) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Function has more than %s parameters: %s", MAX_PARAMETER_COUNT, function.getParameters().size()));
        }
        for (Parameter parameter : function.getParameters()) {
            checkFieldLength("Parameter name", parameter.getName(), MAX_PARAMETER_NAME_LENGTH);
        }

        checkFieldLength(
                "Parameter type list",
                function.getFunctionId().getArgumentTypes().stream()
                        .map(TypeSignature::toString)
                        .collect(joining(",")),
                MAX_PARAMETER_TYPES_LENGTH);
        checkFieldLength("Return type", function.getSignature().getReturnType().toString(), MAX_RETURN_TYPE_LENGTH);

        jdbi.useTransaction(handle -> {
            FunctionNamespaceDao transactionDao = handle.attach(functionNamespaceDaoClass);
            Optional<SqlInvokedFunctionRecord> latestVersion = transactionDao.getLatestRecordForUpdate(hash(function.getFunctionId()), function.getFunctionId());
            if (!replace && latestVersion.isPresent() && !latestVersion.get().isDeleted()) {
                throw new PrestoException(ALREADY_EXISTS, "Function already exists: " + function.getFunctionId());
            }

            if (!latestVersion.isPresent() || !latestVersion.get().getFunction().hasSameDefinitionAs(function)) {
                long newVersion = latestVersion.map(SqlInvokedFunctionRecord::getFunction).map(MySqlFunctionNamespaceManager::getLongVersion).orElse(0L) + 1;
                insertSqlInvokedFunction(transactionDao, function, newVersion);
            }
            else if (latestVersion.get().isDeleted()) {
                SqlInvokedFunction latest = latestVersion.get().getFunction();
                checkState(latest.hasVersion(), "Function version missing: %s", latest.getFunctionId());
                transactionDao.setDeletionStatus(hash(latest.getFunctionId()), latest.getFunctionId(), getLongVersion(latest), false);
            }
        });
        refreshFunctionsCache(functionName);
    }

    @Override
    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        checkCatalog(functionName);
        jdbi.useTransaction(handle -> {
            FunctionNamespaceDao transactionDao = handle.attach(functionNamespaceDaoClass);
            List<SqlInvokedFunction> functions = getSqlFunctions(transactionDao, functionName, parameterTypes);

            checkUnique(functions, functionName);
            checkExists(functions, functionName, parameterTypes);

            SqlInvokedFunction latest = functions.get(0);
            RoutineCharacteristics.Builder routineCharacteristics = RoutineCharacteristics.builder(latest.getRoutineCharacteristics());
            alterRoutineCharacteristics.getNullCallClause().ifPresent(routineCharacteristics::setNullCallClause);
            SqlInvokedFunction altered = new SqlInvokedFunction(
                    latest.getFunctionId().getFunctionName(),
                    latest.getParameters(),
                    latest.getSignature().getReturnType(),
                    latest.getDescription(),
                    routineCharacteristics.build(),
                    latest.getBody(),
                    notVersioned());
            if (!altered.hasSameDefinitionAs(latest)) {
                checkState(latest.hasVersion(), "Function version missing: %s", latest.getFunctionId());
                insertSqlInvokedFunction(transactionDao, altered, getLongVersion(latest) + 1);
            }
        });
        refreshFunctionsCache(functionName);
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        checkCatalog(functionName);
        jdbi.useTransaction(handle -> {
            FunctionNamespaceDao transactionDao = handle.attach(functionNamespaceDaoClass);
            List<SqlInvokedFunction> functions = getSqlFunctions(transactionDao, functionName, parameterTypes);

            checkExists(functions, functionName, parameterTypes);

            if (!parameterTypes.isPresent()) {
                transactionDao.setDeleted(functionName.getCatalogName(), functionName.getSchemaName(), functionName.getObjectName());
            }
            else {
                SqlInvokedFunction latest = getOnlyElement(functions);
                checkState(latest.hasVersion(), "Function version missing: %s", latest.getFunctionId());
                transactionDao.setDeletionStatus(hash(latest.getFunctionId()), latest.getFunctionId(), getLongVersion(latest), true);
            }
        });

        refreshFunctionsCache(functionName);
    }

    private List<SqlInvokedFunction> getSqlFunctions(FunctionNamespaceDao functionNamespaceDao, QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes)
    {
        List<SqlInvokedFunctionRecord> records = new ArrayList<>();
        if (parameterTypes.isPresent()) {
            SqlFunctionId functionId = new SqlFunctionId(functionName, parameterTypes.get());
            functionNamespaceDao.getLatestRecordForUpdate(hash(functionId), functionId).ifPresent(records::add);
        }
        else {
            records = functionNamespaceDao.getLatestRecordsForUpdate(functionName.getCatalogName(), functionName.getSchemaName(), functionName.getObjectName());
        }
        return records.stream()
                .filter(record -> !record.isDeleted())
                .map(SqlInvokedFunctionRecord::getFunction)
                .collect(toImmutableList());
    }

    private void insertSqlInvokedFunction(FunctionNamespaceDao functionNamespaceDao, SqlInvokedFunction function, long version)
    {
        QualifiedObjectName functionName = function.getFunctionId().getFunctionName();
        functionNamespaceDao.insertFunction(
                hash(function.getFunctionId()),
                function.getFunctionId(),
                version,
                functionName.getCatalogName(),
                functionName.getSchemaName(),
                functionName.getObjectName(),
                function.getParameters(),
                function.getSignature().getReturnType(),
                function.getDescription(),
                function.getRoutineCharacteristics(),
                function.getBody());
    }

    private static long getLongVersion(SqlFunctionHandle functionHandle)
    {
        return parseLong(functionHandle.getVersion());
    }

    private static long getLongVersion(SqlInvokedFunction function)
    {
        return parseLong(function.getRequiredVersion());
    }

    private static void checkFieldLength(String fieldName, String field, int maxLength)
    {
        if (field.length() > maxLength) {
            throw new PrestoException(GENERIC_USER_ERROR, format("%s exceeds max length of %s: %s", fieldName, maxLength, field));
        }
    }

    private static void checkUnique(List<SqlInvokedFunction> functions, QualifiedObjectName functionName)
    {
        if (functions.size() > 1) {
            String signatures = functions.stream()
                    .map(SqlFunction::getSignature)
                    .map(Signature::toString)
                    .collect(joining("; "));
            throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, format("Function '%s' has multiple signatures: %s. Please specify parameter types.", functionName, signatures));
        }
    }

    private static void checkExists(List<SqlInvokedFunction> functions, QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes)
    {
        if (functions.isEmpty()) {
            String formattedParameterTypes = parameterTypes.map(types -> types.stream()
                    .map(TypeSignature::toString)
                    .collect(joining(",", "(", ")"))).orElse("");
            throw new PrestoException(NOT_FOUND, format("Function not found: %s%s", functionName, formattedParameterTypes));
        }
    }

    private static String hash(SqlFunctionId functionId)
    {
        return sha256().hashString(functionId.toString(), UTF_8).toString();
    }
}
