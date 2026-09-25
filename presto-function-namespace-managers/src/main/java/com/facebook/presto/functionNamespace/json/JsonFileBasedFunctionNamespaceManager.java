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
import com.facebook.presto.common.type.StandardTypes;
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
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.LongVariableConstraint;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.google.common.base.Preconditions.checkArgument;
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
                function.getSignature().getTypeVariableConstraints(),
                function.getSignature().getLongVariableConstraints(),
                function.getSignature().getReturnType(),
                function.getDescription(),
                function.getRoutineCharacteristics(),
                function.getBody(),
                function.getVariableArity(),
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
        QualifiedObjectName qualifiedFunctionName = QualifiedObjectName.valueOf(new CatalogSchemaName(getCatalogName(), jsonBasedUdfFunctionMetaData.getSchema()), functionName);
        List<String> parameterNameList = jsonBasedUdfFunctionMetaData.getParamNames();
        List<TypeSignature> parameterTypeList = jsonBasedUdfFunctionMetaData.getParamTypes();

        boolean variableArity = jsonBasedUdfFunctionMetaData.getVariableArity();
        validateAnyType(qualifiedFunctionName, parameterTypeList, variableArity);

        ImmutableList.Builder<Parameter> parameterBuilder = ImmutableList.builder();
        for (int i = 0; i < parameterNameList.size(); i++) {
            parameterBuilder.add(new Parameter(parameterNameList.get(i), parameterTypeList.get(i)));
        }

        List<TypeVariableConstraint> typeVariableConstraints = jsonBasedUdfFunctionMetaData.getTypeVariableConstraints().orElse(ImmutableList.of());
        List<LongVariableConstraint> longVariableConstraints = jsonBasedUdfFunctionMetaData.getLongVariableConstraints().orElse(ImmutableList.of());

        return new SqlInvokedFunction(
                qualifiedFunctionName,
                parameterBuilder.build(),
                typeVariableConstraints,
                longVariableConstraints,
                jsonBasedUdfFunctionMetaData.getOutputType(),
                jsonBasedUdfFunctionMetaData.getDocString(),
                jsonBasedUdfFunctionMetaData.getRoutineCharacteristics(),
                jsonBasedUdfFunctionMetaData.getBody().orElse(""),
                variableArity,
                notVersioned(),
                jsonBasedUdfFunctionMetaData.getFunctionKind(),
                jsonBasedUdfFunctionMetaData.getAggregateMetadata());
    }

    // The "any" sentinel type is only meaningful as the tail of a variable-arity signature (each
    // trailing argument may then be of any type). Reject any other usage so it cannot leak into a
    // non-variadic or non-tail position, where it would fail to bind in a confusing way.
    private static void validateAnyType(QualifiedObjectName functionName, List<TypeSignature> parameterTypes, boolean variableArity)
    {
        for (int i = 0; i < parameterTypes.size(); i++) {
            if (StandardTypes.ANY.equals(parameterTypes.get(i).getBase())) {
                if (!variableArity || i != parameterTypes.size() - 1) {
                    throw new PrestoException(
                            GENERIC_USER_ERROR,
                            format("Function '%s' uses type '%s', which is only allowed as the last argument of a variable-arity function", functionName, StandardTypes.ANY));
                }
            }
        }
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
        return toMetadata(function, function.getSignature().getArgumentTypes());
    }

    // Builds metadata using the supplied argument types rather than the declared ones. For a
    // variable-arity call the supplied types are the call-arity (expanded) types, so the analyzer's
    // per-argument checks line up with the actual call site.
    private FunctionMetadata toMetadata(SqlInvokedFunction function, List<TypeSignature> argumentTypes)
    {
        return new FunctionMetadata(
                function.getSignature().getName(),
                argumentTypes,
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
                function.getDescription(),
                anyVariadicTailPosition(function));
    }

    // For a variable-arity function whose declared tail is the internal __ANY__ sentinel, returns the
    // index where the trailing (any-typed) arguments begin. The RowExpression translator uses this to
    // fold the tail into a single JSON argument. Empty for all other functions.
    private static OptionalInt anyVariadicTailPosition(SqlInvokedFunction function)
    {
        List<TypeSignature> declaredArgumentTypes = function.getSignature().getArgumentTypes();
        if (function.getVariableArity()
                && !declaredArgumentTypes.isEmpty()
                && StandardTypes.ANY.equals(declaredArgumentTypes.get(declaredArgumentTypes.size() - 1).getBase())) {
            return OptionalInt.of(declaredArgumentTypes.size() - 1);
        }
        return OptionalInt.empty();
    }

    @Override
    protected UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName)
    {
        return userDefinedTypes.get(typeName);
    }

    @Override
    protected FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
    {
        return toMetadata(resolveFunctionForHandle(functionHandle), functionHandle.getFunctionId().getArgumentTypes());
    }

    // A variable-arity call's argument types are expanded during binding, so its handle does not
    // match any declared function by exact handle equality. Fall back to the declared variable-arity
    // function whose signature accepts the (call-arity) argument types carried by the handle.
    private SqlInvokedFunction resolveFunctionForHandle(SqlFunctionHandle functionHandle)
    {
        Collection<SqlInvokedFunction> candidates = fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName());
        List<TypeSignature> argumentTypes = functionHandle.getFunctionId().getArgumentTypes();
        return candidates.stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .findFirst()
                .orElseGet(() -> candidates.stream()
                        .filter(SqlInvokedFunction::getVariableArity)
                        .filter(function -> variableArityAccepts(function.getSignature().getArgumentTypes(), argumentTypes))
                        .collect(onlyElement()));
    }

    @Override
    public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        SqlFunctionId functionId = new SqlFunctionId(signature.getName(), signature.getArgumentTypes());
        if (!latestFunctions.containsKey(functionId)) {
            // Variable-arity call: bind to the declared variable-arity function and carry the
            // call-arity argument types in the handle (mirrors the native function namespace manager).
            SqlInvokedFunction variableArityFunction = findVariableArityFunction(signature);
            if (variableArityFunction != null) {
                return new SqlFunctionHandle(functionId, variableArityFunction.getRequiredVersion());
            }
        }
        return super.getFunctionHandle(transactionHandle, signature);
    }

    private SqlInvokedFunction findVariableArityFunction(Signature signature)
    {
        return latestFunctions.values().stream()
                .filter(function -> function.getSignature().getName().equals(signature.getName()))
                .filter(SqlInvokedFunction::getVariableArity)
                .filter(function -> variableArityAccepts(function.getSignature().getArgumentTypes(), signature.getArgumentTypes()))
                .findFirst()
                .orElse(null);
    }

    // Whether a declared variable-arity signature (fixed head + repeated/any tail) accepts the given
    // (already-bound) call-arity argument types.
    private static boolean variableArityAccepts(List<TypeSignature> declared, List<TypeSignature> actual)
    {
        if (declared.isEmpty() || actual.size() < declared.size() - 1) {
            return false;
        }
        for (int i = 0; i < declared.size() - 1; i++) {
            if (!declared.get(i).equals(actual.get(i))) {
                return false;
            }
        }
        TypeSignature tail = declared.get(declared.size() - 1);
        if (StandardTypes.ANY.equals(tail.getBase())) {
            return true;
        }
        for (int i = declared.size() - 1; i < actual.size(); i++) {
            if (!tail.equals(actual.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
    {
        return sqlInvokedFunctionToImplementation(resolveFunctionForHandle(functionHandle));
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
