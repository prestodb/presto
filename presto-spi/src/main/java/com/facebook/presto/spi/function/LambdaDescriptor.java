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

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class LambdaDescriptor
{
    /**
     * Index of the argument in the Call expression of the lambda function that this LambdaDescriptor represents
     */
    final int callArgumentIndex;

    /**
     * Map of lambda argument descriptors where the key corresponds to the index in the list of lambda argument and value is the descriptor of the argument.
     */
    final Map<Integer, LambdaArgumentDescriptor> lambdaArgumentDescriptors;

    public LambdaDescriptor(int callArgumentIndex, Map<Integer, LambdaArgumentDescriptor> lambdaArgumentDescriptors)
    {
        this.callArgumentIndex = callArgumentIndex;
        this.lambdaArgumentDescriptors = requireNonNull(lambdaArgumentDescriptors, "lambdaArgumentDescriptors is null");
    }

    public int getCallArgumentIndex()
    {
        return callArgumentIndex;
    }

    public Map<Integer, LambdaArgumentDescriptor> getLambdaArgumentDescriptors()
    {
        return lambdaArgumentDescriptors;
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
        LambdaDescriptor that = (LambdaDescriptor) o;
        return callArgumentIndex == that.callArgumentIndex && Objects.equals(lambdaArgumentDescriptors, that.lambdaArgumentDescriptors);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(callArgumentIndex, lambdaArgumentDescriptors);
    }
}
