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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.Subfield;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class LambdaArgumentDescriptor
{
    /**
     * Index of the function argument that contains the function input (Array or Map), to which this lambda argument relate to.
     */
    final int callArgumentIndex;

    /**
     * Contains the transformation function between the subfields of this lambda argument and the input of the function.
     *
     * The reason this transformation is needed because the input of the function is the Array or Map, while the lambda arguments are the element of the array or key/value of
     * the map. Specifically for map, the transformation function for the lambda argument of the map key will be different from the transformation of the map value.
     * If transformation succeeded, then the returned value contains the transformed set of lambda subfields. Otherwise, the function must return <code>Optional.empty()</code>
     * value.
     */
    private final Function<Set<Subfield>, Set<Subfield>> lambdaArgumentToInputTransformationFunction;

    public LambdaArgumentDescriptor(int callArgumentIndex, Function<Set<Subfield>, Set<Subfield>> lambdaArgumentToInputTransformationFunction)
    {
        this.callArgumentIndex = callArgumentIndex;
        this.lambdaArgumentToInputTransformationFunction = requireNonNull(lambdaArgumentToInputTransformationFunction, "lambdaArgumentToInputTransformationFunction is null");
    }

    public int getCallArgumentIndex()
    {
        return callArgumentIndex;
    }

    public Function<Set<Subfield>, Set<Subfield>> getLambdaArgumentToInputTransformationFunction()
    {
        return lambdaArgumentToInputTransformationFunction;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LambdaArgumentDescriptor that = (LambdaArgumentDescriptor) o;
        return callArgumentIndex == that.callArgumentIndex && Objects.equals(lambdaArgumentToInputTransformationFunction, that.lambdaArgumentToInputTransformationFunction);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(callArgumentIndex, lambdaArgumentToInputTransformationFunction);
    }
}
