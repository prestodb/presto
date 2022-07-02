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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.spi.function.AccumulatorFunctions;

import java.lang.invoke.MethodHandle;

import static java.util.Objects.requireNonNull;

public class HiveAccumulatorFunctions
        implements AccumulatorFunctions
{
    private final MethodHandle inputFunction;
    private final MethodHandle combineFunction;
    private final MethodHandle outputFunction;

    public HiveAccumulatorFunctions(MethodHandle inputFunction, MethodHandle combineFunction, MethodHandle outputFunction)
    {
        this.inputFunction = requireNonNull(inputFunction);
        this.combineFunction = requireNonNull(combineFunction);
        this.outputFunction = requireNonNull(outputFunction);
    }

    @Override
    public MethodHandle getInputFunction()
    {
        return inputFunction;
    }

    @Override
    public MethodHandle getCombineFunction()
    {
        return combineFunction;
    }

    @Override
    public MethodHandle getOutputFunction()
    {
        return outputFunction;
    }
}
