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

import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.QualifiedName;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static java.util.Objects.requireNonNull;

@ThreadSafe
class FunctionNamespace
{
    private final FunctionRegistry registry;

    public FunctionNamespace(FunctionRegistry registry)
    {
        this.registry = requireNonNull(registry, "registry is null");
    }

    public void addFunctions(List<? extends SqlFunction> functions)
    {
        registry.addFunctions(functions);
    }

    public List<SqlFunction> listFunctions()
    {
        return registry.list();
    }

    public Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return registry.resolveFunction(name, parameterTypes);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(Signature signature)
    {
        return registry.getWindowFunctionImplementation(signature);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(Signature signature)
    {
        return registry.getAggregateFunctionImplementation(signature);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        return registry.getScalarFunctionImplementation(signature);
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return registry.isAggregationFunction(name);
    }

    public boolean canResolveOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes)
    {
        return registry.canResolveOperator(operatorType, returnType, argumentTypes);
    }

    public Signature resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return registry.resolveOperator(operatorType, argumentTypes);
    }

    public Signature getCoercion(TypeSignature fromType, TypeSignature toType)
    {
        return registry.getCoercion(fromType, toType);
    }

    public boolean isRegistered(Signature signature)
    {
        return registry.isRegistered(signature);
    }
}
