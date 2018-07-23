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
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.SqlPathElement;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionUtils.isOperator;
import static com.facebook.presto.metadata.Signature.qualifySignature;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FunctionManager
{
    private static final String TEMP_DEFAULT_CATALOG = "system";
    private static final String TEMP_DEFAULT_SCHEMA = "functions";

    private volatile Map<String, FunctionNamespace> functionNamespaces;
    private final FunctionNamespace operatorNamespace;
    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final FeaturesConfig featuresConfig;

    public FunctionManager(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, FeaturesConfig featuresConfig)
    {
        functionNamespaces = ImmutableMap.of();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.featuresConfig = requireNonNull(featuresConfig, "featuresConfig is null");
        operatorNamespace = new StaticFunctionNamespace(typeManager, blockEncodingSerde, featuresConfig);
    }

    public void addFunctions(List<? extends SqlFunction> functions)
    {
        addFunctions(TEMP_DEFAULT_CATALOG, functions);
    }

    public synchronized void addFunctions(String catalog, List<? extends SqlFunction> functions)
    {
        Map<Boolean, List<SqlFunction>> operatorAndFunctionMap = functions.stream()
                .collect(Collectors.partitioningBy(function -> isOperator(function.getSignature())));

        operatorNamespace.addFunctions(operatorAndFunctionMap.get(true));

        if (!functionNamespaces.containsKey(catalog)) {
            FunctionNamespace namespace = new StaticFunctionNamespace(typeManager, blockEncodingSerde, featuresConfig);
            namespace.addFunctions(operatorAndFunctionMap.get(false));
            functionNamespaces = ImmutableMap.<String, FunctionNamespace>builder()
                    .putAll(functionNamespaces)
                    .put(catalog, namespace)
                    .build();
        }
        else {
            functionNamespaces.get(catalog).addFunctions(operatorAndFunctionMap.get(false));
        }
    }

    public List<SqlFunction> list(Session session)
    {
        ImmutableList.Builder<SqlFunction> builder = ImmutableList.builder();
        builder.addAll(operatorNamespace.list());
        for (SqlPathElement element : session.getPath().getParsedPath()) {
            FunctionNamespace namespace = functionNamespaces.get(getCatalog(element, session));
            if (namespace != null) {
                builder.addAll(namespace.list());
            }
        }
        return builder.build();
    }

    public Signature resolveFunction(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        try {
            return operatorNamespace.resolveFunction(name, parameterTypes);
        }
        catch (Exception e) {
            //do nothing and try to resolve the function using the path
        }
        for (SqlPathElement element : session.getPath().getParsedPath()) {
            FunctionNamespace namespace = functionNamespaces.get(getCatalog(element, session));
            if (namespace != null) {
                try {
                    Signature signature = namespace.resolveFunction(name, parameterTypes);
                    return qualifySignature(signature, getCatalog(element, session), TEMP_DEFAULT_SCHEMA);
                }
                catch (PrestoException e) {
                    if (!e.getErrorCode().equals(FUNCTION_NOT_FOUND.toErrorCode())) {
                        throw e;
                    }
                }
            }
        }
        throw new PrestoException(FUNCTION_NOT_FOUND, format("Function %s not found in path: %s", name, session.getPath()));
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(Signature signature)
    {
        try {
            return operatorNamespace.getWindowFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            //do nothing and try to resolve normally
        }
        if (!signature.getCatalog().isPresent() || !signature.getSchema().isPresent()) {
            throw new PrestoException(NOT_FOUND, format("Signature %s does not have both catalog and schema set for resolution", signature));
        }
        return functionNamespaces.get(signature.getCatalog().get()).getWindowFunctionImplementation(signature);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(Signature signature)
    {
        try {
            return operatorNamespace.getAggregateFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            //do nothing and try to resolve normally
        }
        if (!signature.getCatalog().isPresent() || !signature.getSchema().isPresent()) {
            throw new PrestoException(NOT_FOUND, format("Signature %s does not have both catalog and schema set for resolution", signature));
        }
        return functionNamespaces.get(signature.getCatalog().get()).getAggregateFunctionImplementation(signature);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        try {
            return operatorNamespace.getScalarFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            //do nothing and try to resolve normally
        }
        if (!signature.getCatalog().isPresent() || !signature.getSchema().isPresent()) {
            throw new PrestoException(NOT_FOUND, format("Signature %s does not have both catalog and schema set for resolution", signature));
        }
        return functionNamespaces.get(signature.getCatalog().get()).getScalarFunctionImplementation(signature);
    }

    public boolean canResolveOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes)
    {
        return operatorNamespace.canResolveOperator(operatorType, returnType, argumentTypes);
    }

    public Signature resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        try {
            return operatorNamespace.resolveOperator(operatorType, argumentTypes);
        }
        catch (PrestoException e) {
            if (!(e instanceof OperatorNotFoundException)) {
                throw e;
            }
        }
        throw new OperatorNotFoundException(
                operatorType,
                argumentTypes.stream()
                        .map(Type::getTypeSignature)
                        .collect(toImmutableList()));
    }

    public Signature getCoercion(Type fromType, Type toType)
    {
        return getCoercion(fromType.getTypeSignature(), toType.getTypeSignature());
    }

    public Signature getCoercion(TypeSignature fromType, TypeSignature toType)
    {
        try {
            return operatorNamespace.getCoercion(fromType, toType);
        }
        catch (PrestoException e) {
            if (!(e instanceof OperatorNotFoundException)) {
                throw e;
            }
        }
        throw new OperatorNotFoundException(OperatorType.CAST, ImmutableList.of(fromType), toType);
    }

    private String getCatalog(SqlPathElement pathElement, Session session)
    {
        if (pathElement.getCatalog().isPresent()) {
            return pathElement.getCatalog().get().getValue();
        }
        if (session.getCatalog().isPresent()) {
            return session.getCatalog().get();
        }
        throw new PrestoException(NOT_FOUND, format("Catalog not found in path element %s and not set in session %s", pathElement, session));
    }
}
