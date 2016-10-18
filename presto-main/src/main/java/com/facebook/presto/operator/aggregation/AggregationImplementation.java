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

import com.facebook.presto.metadata.Signature;

import java.lang.reflect.Method;

import static java.util.Objects.requireNonNull;

public class AggregationImplementation
{
    private final Signature signature;

    private final Class<?> definitionClass;
    private final Class<?> stateClass;
    private final Method inputFunction;
    private final Method outputFunction;

    public AggregationImplementation(Signature signature, Class<?> definitionClass, Class<?> stateClass, Method inputFunction, Method outputFunction)
    {
        this.signature = requireNonNull(signature, "signature cannot be null");
        this.definitionClass = requireNonNull(definitionClass, "definition class cannot be null");
        this.stateClass = requireNonNull(stateClass, "stateClass cannot be null");
        this.inputFunction = requireNonNull(inputFunction, "inputFunction cannot be null");
        this.outputFunction = requireNonNull(outputFunction, "outputFunction cannot be null");
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
}
