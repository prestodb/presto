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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.function.QualifiedFunctionName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.functionNamespace.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.InvalidFunctionHandleException;
import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.sha256;
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
            @ServingCatalog String catalogName)
    {
        super(catalogName, managerConfig);
        this.jdbi = requireNonNull(jdbi, "jdbi is null");
        this.functionNamespaceDao = requireNonNull(functionNamespaceDao, "functionNamespaceDao is null");
        this.functionNamespaceDaoClass = requireNonNull(functionNamespaceDaoClass, "functionNamespaceDaoClass is null");
    }

    @PostConstruct
    public void initialize()
    {
        functionNamespaceDao.createFunctionNamespacesTableIfNotExists();
        functionNamespaceDao.createSqlFunctionsTableIfNotExists();
    }

    @Override
    public Collection<SqlInvokedFunction> listFunctions()
    {
        return functionNamespaceDao.listFunctions(getCatalogName());
    }

    @Override
    protected Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedFunctionName functionName)
    {
        checkCatalog(functionName);
        return functionNamespaceDao.getFunctions(
                functionName.getFunctionNamespace().getCatalogName(),
                functionName.getFunctionNamespace().getSchemaName(),
                functionName.getFunctionName());
    }

    @Override
    protected FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        Optional<SqlInvokedFunction> function = functionNamespaceDao.getFunction(hash(functionHandle.getFunctionId()), functionHandle.getFunctionId(), functionHandle.getVersion());
        if (!function.isPresent()) {
            throw new InvalidFunctionHandleException(functionHandle);
        }
        return sqlInvokedFunctionToMetadata(function.get());
    }

    @Override
    protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        Optional<SqlInvokedFunction> function = functionNamespaceDao.getFunction(hash(functionHandle.getFunctionId()), functionHandle.getFunctionId(), functionHandle.getVersion());
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
        checkArgument(!function.getVersion().isPresent(), "function '%s' is already versioned", function);

        QualifiedFunctionName functionName = function.getFunctionId().getFunctionName();
        checkFieldLength("Catalog name", functionName.getFunctionNamespace().getCatalogName(), MAX_CATALOG_NAME_LENGTH);
        checkFieldLength("Schema name", functionName.getFunctionNamespace().getSchemaName(), MAX_SCHEMA_NAME_LENGTH);
        if (!functionNamespaceDao.functionNamespaceExists(functionName.getFunctionNamespace().getCatalogName(), functionName.getFunctionNamespace().getSchemaName())) {
            throw new PrestoException(NOT_FOUND, format("Function namespace not found: %s", functionName.getFunctionNamespace()));
        }
        checkFieldLength("Function name", functionName.getFunctionName(), MAX_FUNCTION_NAME_LENGTH);

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
                long newVersion = latestVersion.map(SqlInvokedFunctionRecord::getFunction).flatMap(SqlInvokedFunction::getVersion).orElse(0L) + 1;
                insertSqlInvokedFunction(transactionDao, function, newVersion);
            }
            else if (latestVersion.get().isDeleted()) {
                SqlInvokedFunction latest = latestVersion.get().getFunction();
                checkState(latest.getVersion().isPresent(), "Function version missing: %s", latest.getFunctionId());
                transactionDao.setDeletionStatus(hash(latest.getFunctionId()), latest.getFunctionId(), latest.getVersion().get(), false);
            }
        });
        refreshFunctionsCache(functionName);
    }

    @Override
    public void alterFunction(QualifiedFunctionName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
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
                    Optional.empty());
            if (!altered.hasSameDefinitionAs(latest)) {
                checkState(latest.getVersion().isPresent(), "Function version missing: %s", latest.getFunctionId());
                insertSqlInvokedFunction(transactionDao, altered, latest.getVersion().get() + 1);
            }
        });
        refreshFunctionsCache(functionName);
    }

    @Override
    public void dropFunction(QualifiedFunctionName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        checkCatalog(functionName);
        jdbi.useTransaction(handle -> {
            FunctionNamespaceDao transactionDao = handle.attach(functionNamespaceDaoClass);
            List<SqlInvokedFunction> functions = getSqlFunctions(transactionDao, functionName, parameterTypes);

            checkUnique(functions, functionName);
            checkExists(functions, functionName, parameterTypes);

            SqlInvokedFunction latest = functions.get(0);
            checkState(latest.getVersion().isPresent(), "Function version missing: %s", latest.getFunctionId());
            transactionDao.setDeletionStatus(hash(latest.getFunctionId()), latest.getFunctionId(), latest.getVersion().get(), true);
        });

        refreshFunctionsCache(functionName);
    }

    private List<SqlInvokedFunction> getSqlFunctions(FunctionNamespaceDao functionNamespaceDao, QualifiedFunctionName functionName, Optional<List<TypeSignature>> parameterTypes)
    {
        List<SqlInvokedFunctionRecord> records = new ArrayList<>();
        if (parameterTypes.isPresent()) {
            SqlFunctionId functionId = new SqlFunctionId(functionName, parameterTypes.get());
            functionNamespaceDao.getLatestRecordForUpdate(hash(functionId), functionId).ifPresent(records::add);
        }
        else {
            CatalogSchemaName functionNamespace = functionName.getFunctionNamespace();
            records = functionNamespaceDao.getLatestRecordsForUpdate(functionNamespace.getCatalogName(), functionNamespace.getSchemaName(), functionName.getFunctionName());
        }
        return records.stream()
                .filter(record -> !record.isDeleted())
                .map(SqlInvokedFunctionRecord::getFunction)
                .collect(toImmutableList());
    }

    private void insertSqlInvokedFunction(FunctionNamespaceDao functionNamespaceDao, SqlInvokedFunction function, long version)
    {
        QualifiedFunctionName functionName = function.getFunctionId().getFunctionName();
        functionNamespaceDao.insertFunction(
                hash(function.getFunctionId()),
                function.getFunctionId(),
                version,
                functionName.getFunctionNamespace().getCatalogName(),
                functionName.getFunctionNamespace().getSchemaName(),
                functionName.getFunctionName(),
                function.getParameters(),
                function.getSignature().getReturnType(),
                function.getDescription(),
                function.getRoutineCharacteristics(),
                function.getBody());
    }

    private static void checkFieldLength(String fieldName, String field, int maxLength)
    {
        if (field.length() > maxLength) {
            throw new PrestoException(GENERIC_USER_ERROR, format("%s exceeds max length of %s: %s", fieldName, maxLength, field));
        }
    }

    private static void checkUnique(List<SqlInvokedFunction> functions, QualifiedFunctionName functionName)
    {
        if (functions.size() > 1) {
            String signatures = functions.stream()
                    .map(SqlFunction::getSignature)
                    .map(Signature::toString)
                    .collect(joining("; "));
            throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, format("Function '%s' has multiple signatures: %s. Please specify parameter types.", functionName, signatures));
        }
    }

    private static void checkExists(List<SqlInvokedFunction> functions, QualifiedFunctionName functionName, Optional<List<TypeSignature>> parameterTypes)
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
