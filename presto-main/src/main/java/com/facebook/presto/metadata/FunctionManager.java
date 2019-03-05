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
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.Signature;
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
        this.functionInvokerProvider = new FunctionInvokerProvider(this);
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

    public FunctionHandle resolveFunction(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        // TODO Actually use session
        // Session will be used to provide information about the order of function namespaces to through resolving the function.
        // This is likely to be in terms of SQL path. Currently we still don't have support multiple function namespaces, nor
        // SQL path. As a result, session is not used here. We still add this to distinguish the two versions of resolveFunction
        // while the refactoring is on-going.
        return globalFunctionNamespace.resolveFunction(name, parameterTypes);
    }

    public Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return globalFunctionNamespace.resolveFunction(name, parameterTypes).getSignature();
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionHandle functionHandle)
    {
        return globalFunctionNamespace.getWindowFunctionImplementation(functionHandle);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        return globalFunctionNamespace.getAggregateFunctionImplementation(functionHandle);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        return globalFunctionNamespace.getScalarFunctionImplementation(functionHandle.getSignature());
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        return globalFunctionNamespace.getScalarFunctionImplementation(signature);
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return globalFunctionNamespace.isAggregationFunction(name);
    }

    public FunctionHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        return globalFunctionNamespace.resolveOperator(operatorType, argumentTypes);
    }

    public boolean isRegistered(Signature signature)
    {
        return globalFunctionNamespace.isRegistered(signature);
    }

    public FunctionHandle lookupCast(OperatorType castType, TypeSignature fromType, TypeSignature toType)
    {
        return globalFunctionNamespace.lookupCast(castType, fromType, toType);
    }
}
