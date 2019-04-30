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
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.OperatorType;
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
        implements FunctionMetadataManager
{
    private final StaticFunctionNamespace staticFunctionNamespace;
    private final FunctionInvokerProvider functionInvokerProvider;

    public FunctionManager(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, FeaturesConfig featuresConfig)
    {
        this.staticFunctionNamespace = new StaticFunctionNamespace(typeManager, blockEncodingSerde, featuresConfig, this);
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
        staticFunctionNamespace.addFunctions(functions);
    }

    public List<SqlFunction> listFunctions()
    {
        return staticFunctionNamespace.listFunctions();
    }

    /**
     * Resolves a function using the SQL path, and implicit type coercions.
     *
     * @throws PrestoException if there are no matches or multiple matches
     */
    public FunctionHandle resolveFunction(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        // TODO Actually use session
        // Session will be used to provide information about the order of function namespaces to through resolving the function.
        // This is likely to be in terms of SQL path. Currently we still don't have support multiple function namespaces, nor
        // SQL path. As a result, session is not used here. We still add this to distinguish the two versions of resolveFunction
        // while the refactoring is on-going.
        return staticFunctionNamespace.resolveFunction(name, parameterTypes);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        return staticFunctionNamespace.getFunctionMetadata(functionHandle);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionHandle functionHandle)
    {
        return staticFunctionNamespace.getWindowFunctionImplementation(functionHandle);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        return staticFunctionNamespace.getAggregateFunctionImplementation(functionHandle);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        return staticFunctionNamespace.getScalarFunctionImplementation(functionHandle);
    }

    public FunctionHandle resolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes)
    {
        return staticFunctionNamespace.resolveOperator(operatorType, argumentTypes);
    }

    /**
     * Lookup up a function with name and fully bound types. This can only be used for builtin functions. {@link #resolveFunction(Session, QualifiedName, List)}
     * should be used for dynamically registered functions.
     *
     * @throws PrestoException if function could not be found
     */
    public FunctionHandle lookupFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        return staticFunctionNamespace.lookupFunction(QualifiedName.of(name), parameterTypes);
    }

    public FunctionHandle lookupCast(CastType castType, TypeSignature fromType, TypeSignature toType)
    {
        return staticFunctionNamespace.lookupCast(castType, fromType, toType);
    }
}
