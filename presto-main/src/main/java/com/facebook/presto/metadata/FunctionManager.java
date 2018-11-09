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
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeRegistry;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

@ThreadSafe
public class FunctionManager
{
    private final FunctionNamespace globalFunctionNamespace;
    private final FunctionInvokerProvider functionInvokerProvider;

    public FunctionManager(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, FeaturesConfig featuresConfig)
    {
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, blockEncodingSerde, featuresConfig, this);
        this.globalFunctionNamespace = new FunctionNamespace(functionRegistry);
        this.functionInvokerProvider = new FunctionInvokerProvider(functionRegistry);
        if (typeManager instanceof TypeRegistry) {
            ((TypeRegistry) typeManager).setFunctionManager(this);
        }
    }

    public FunctionInvokerProvider getFunctionInvokerProvider()
    {
        return functionInvokerProvider;
    }

    public void addFunctions(List<? extends SqlFunction> functions)
    {
        globalFunctionNamespace.addFunctions(functions);
    }

    public List<SqlFunction> listFunctions()
    {
        return globalFunctionNamespace.listFunctions();
    }

    public Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return globalFunctionNamespace.resolveFunction(name, parameterTypes);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(Signature signature)
    {
        return globalFunctionNamespace.getWindowFunctionImplementation(signature);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(Signature signature)
    {
        return globalFunctionNamespace.getAggregateFunctionImplementation(signature);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        return globalFunctionNamespace.getScalarFunctionImplementation(signature);
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return globalFunctionNamespace.isAggregationFunction(name);
    }

    public boolean canResolveOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes)
    {
        return globalFunctionNamespace.canResolveOperator(operatorType, returnType, argumentTypes);
    }

    public Signature resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        return globalFunctionNamespace.resolveOperator(operatorType, argumentTypes);
    }

    public boolean isRegistered(Signature signature)
    {
        return globalFunctionNamespace.isRegistered(signature);
    }

    public Signature getCoercion(Type fromType, Type toType)
    {
        return getCoercion(fromType.getTypeSignature(), toType.getTypeSignature());
    }

    public Signature getCoercion(TypeSignature fromType, TypeSignature toType)
    {
        return globalFunctionNamespace.getCoercion(fromType, toType);
    }
}
