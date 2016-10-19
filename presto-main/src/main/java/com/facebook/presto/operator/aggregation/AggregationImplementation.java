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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.TypeManager;

import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AggregationImplementation
{
    private final Signature signature;

    private final Class<?> definitionClass;
    private final Class<?> stateClass;
    private final Method inputFunction;
    private final Method outputFunction;
    private final List<Class<?>> argumentNativeContainerTypes;

    public AggregationImplementation(Signature signature, Class<?> definitionClass, Class<?> stateClass, Method inputFunction, Method outputFunction, List<Class<?>> argumentNativeContainerTypes)
    {
        this.signature = requireNonNull(signature, "signature cannot be null");
        this.definitionClass = requireNonNull(definitionClass, "definition class cannot be null");
        this.stateClass = requireNonNull(stateClass, "stateClass cannot be null");
        this.inputFunction = requireNonNull(inputFunction, "inputFunction cannot be null");
        this.outputFunction = requireNonNull(outputFunction, "outputFunction cannot be null");
        this.argumentNativeContainerTypes = requireNonNull(argumentNativeContainerTypes, "argumentNativeContainerTypes cannot be null");
    }

    public Signature getSignature()
    {
        return signature;
    }

    public Class<?> getDefinitionClass()
    {
        return definitionClass;
    }

    public Class<?> getStateClass()
    {
        return stateClass;
    }

    public Method getInputFunction()
    {
        return inputFunction;
    }

    public Method getOutputFunction()
    {
        return outputFunction;
    }

    public boolean hasSpecializedTypeParameters()
    {
        return false;
    }

    public boolean areTypesAssignable(Signature boundSignature, BoundVariables variables, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkState(argumentNativeContainerTypes.size() == boundSignature.getArgumentTypes().size(), "Number of argument assigned to AggregationImplementation is different than number parsed from annotations.");

        // TODO specialized functions variants support is missing here
        for (int i = 0; i < boundSignature.getArgumentTypes().size(); i++) {
            Class<?> argumentType = typeManager.getType(boundSignature.getArgumentTypes().get(i)).getJavaType();
            // FIXME check if Block argument is really @BlockPosition annotated
            if (!(argumentType.isAssignableFrom(argumentNativeContainerTypes.get(i)) || Block.class.isAssignableFrom(argumentNativeContainerTypes.get(i)))) {
                return false;
            }
        }

        return true;
    }
}
