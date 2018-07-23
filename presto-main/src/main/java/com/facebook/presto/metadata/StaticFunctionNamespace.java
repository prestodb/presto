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
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.QualifiedName;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

@ThreadSafe
public class StaticFunctionNamespace
        implements FunctionNamespace
{
    private final FunctionRegistry functionRegistry;

    public StaticFunctionNamespace(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, FunctionManager functionManager)
    {
        functionRegistry = new FunctionRegistry(typeManager, blockEncodingSerde, functionManager);
    }

    public void addFunctions(List<? extends SqlFunction> functions)
    {
        functionRegistry.addFunctions(functions);
    }

    public List<SqlFunction> list()
    {
        return functionRegistry.list();
    }

    public Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return functionRegistry.resolveFunction(name, parameterTypes);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(Signature signature)
    {
        return functionRegistry.getWindowFunctionImplementation(signature);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(Signature signature)
    {
        return functionRegistry.getAggregateFunctionImplementation(signature);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        return functionRegistry.getScalarFunctionImplementation(signature);
    }

    public boolean canResolveOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes)
    {
        return functionRegistry.canResolveOperator(operatorType, returnType, argumentTypes);
    }

    public Signature resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return functionRegistry.resolveOperator(operatorType, argumentTypes);
    }

    public Signature getCoercion(Type fromType, Type toType)
    {
        return functionRegistry.getCoercion(fromType, toType);
    }

    public Signature getCoercion(TypeSignature fromType, TypeSignature toType)
    {
        return functionRegistry.getCoercion(fromType, toType);
    }
}
