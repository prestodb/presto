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

import java.util.List;

public interface FunctionNamespace
{
    void addFunctions(List<? extends SqlFunction> functions);

    List<SqlFunction> list();

    Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes);

    WindowFunctionSupplier getWindowFunctionImplementation(Signature signature);

    InternalAggregationFunction getAggregateFunctionImplementation(Signature signature);

    ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature);

    boolean canResolveOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes);

    Signature resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException;

    Signature getCoercion(Type fromType, Type toType);

    Signature getCoercion(TypeSignature fromType, TypeSignature toType);

    boolean isAggregationFunction(QualifiedName name);
}
