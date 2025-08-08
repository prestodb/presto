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
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.LongVariableConstraint;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.function.FunctionImplementationType.CPP;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class BuiltInNativeFunctionNamespaceManager
{
    private static final Logger log = Logger.get(BuiltInNativeFunctionNamespaceManager.class);

    private volatile FunctionMap functions = new FunctionMap();
    private final FunctionAndTypeManager functionAndTypeManager;
    private final Supplier<FunctionMap> cachedFunctions =
            Suppliers.memoize(this::checkForNamingConflicts);
    private final LoadingCache<Signature, SpecializedFunctionKey> specializedFunctionKeyCache;
    private final LoadingCache<SpecializedFunctionKey, ScalarFunctionImplementation> specializedScalarCache;
    private final Provider<NativeFunctionDefinitionProvider> nativeFunctionDefinitionProvider;

    public BuiltInNativeFunctionNamespaceManager(FunctionAndTypeManager functionAndTypeManager, Provider<NativeFunctionDefinitionProvider> nativeFunctionDefinitionProvider)
    {
        this.nativeFunctionDefinitionProvider = requireNonNull(nativeFunctionDefinitionProvider, "nativeFunctionDefinitionProvider is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        specializedFunctionKeyCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::doGetSpecializedFunctionKey));
        specializedScalarCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(key -> {
                    checkArgument(
                            key.getFunction() instanceof SqlInvokedFunction,
                            "Unsupported scalar function class: %s",
                            key.getFunction().getClass());
                    return new SqlInvokedScalarFunctionImplementation(((SqlInvokedFunction) key.getFunction()).getBody());
                }));
    }

    public synchronized void registerNativeFunctions()
    {
        List<SqlFunction> allNativeFunctions = nativeFunctionDefinitionProvider
                .get()
                .getUdfDefinition()
                .getUDFSignatureMap()
                .entrySet()
                .stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(metaInfo -> createSqlInvokedFunction(entry.getKey(), metaInfo)))
                .collect(Collectors.toList());
        this.functions = new FunctionMap(this.functions, allNativeFunctions);
    }

    public Collection<? extends SqlFunction> getFunctions(QualifiedObjectName functionName)
    {
        if (functions.list().isEmpty() ||
                (!functionName.getCatalogSchemaName().equals(functionAndTypeManager.getDefaultNamespace()))) {
            return emptyList();
        }
        return cachedFunctions.get().get(functionName);
    }

    public List<SqlFunction> listFunctions()
    {
        return cachedFunctions.get().list();
    }

    public FunctionHandle getFunctionHandle(Signature signature)
    {
        return new BuiltInFunctionHandle(signature, true);
    }

    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        Signature signature = ((BuiltInFunctionHandle) functionHandle).getSignature();
        SpecializedFunctionKey functionKey;
        try {
            functionKey = specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
        SqlFunction function = functionKey.getFunction();
        checkArgument(function instanceof SqlInvokedFunction, "BuiltInPluginFunctionNamespaceManager only support SqlInvokedFunctions");
        SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
        List<String> argumentNames = sqlFunction.getParameters().stream().map(Parameter::getName).collect(toImmutableList());
        return new FunctionMetadata(
                signature.getName(),
                signature.getArgumentTypes(),
                argumentNames,
                signature.getReturnType(),
                signature.getKind(),
                sqlFunction.getRoutineCharacteristics().getLanguage(),
                CPP,
                function.isDeterministic(),
                function.isCalledOnNullInput(),
                sqlFunction.getVersion(),
                sqlFunction.getComplexTypeFunctionDescriptor());
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        return getScalarFunctionImplementation(((BuiltInFunctionHandle) functionHandle).getSignature());
    }

    private ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == SCALAR, "%s is not a scalar function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);

        try {
            return specializedScalarCache.getUnchecked(getSpecializedFunctionKey(signature));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private synchronized FunctionMap checkForNamingConflicts()
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager =
                functionAndTypeManager.getServingFunctionNamespaceManager(functionAndTypeManager.getDefaultNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for catalog '%s'", functionAndTypeManager.getDefaultNamespace().getCatalogName());
        checkForNamingConflicts(functionNamespaceManager.get().listFunctions(Optional.empty(), Optional.empty()));
        return functions;
    }

    private synchronized void checkForNamingConflicts(Collection<? extends SqlFunction> functions)
    {
    }

    private SpecializedFunctionKey doGetSpecializedFunctionKey(Signature signature)
    {
        return functionAndTypeManager.getSpecializedFunctionKey(signature, getFunctions(signature.getName()));
    }

    private SpecializedFunctionKey getSpecializedFunctionKey(Signature signature)
    {
        try {
            return specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    protected synchronized SqlInvokedFunction createSqlInvokedFunction(String functionName, JsonBasedUdfFunctionMetadata jsonBasedUdfFunctionMetaData)
    {
        checkState(jsonBasedUdfFunctionMetaData.getRoutineCharacteristics().getLanguage().equals(RoutineCharacteristics.Language.CPP), "NativeFunctionNamespaceManager only supports CPP UDF");
        QualifiedObjectName qualifiedFunctionName = QualifiedObjectName.valueOf(new CatalogSchemaName("presto", jsonBasedUdfFunctionMetaData.getSchema()), functionName);
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

    public static TypeSignature convertApplicableTypeToVariable(TypeSignature typeSignature)
    {
        List<TypeSignature> typeSignaturesList = convertApplicableTypeToVariable(ImmutableList.of(typeSignature));
        checkArgument(!typeSignaturesList.isEmpty(), "Type signature list is empty for : " + typeSignature);
        return typeSignaturesList.get(0);
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
}
