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
package com.facebook.presto.operator;

import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.function.WindowFunction;
import com.facebook.presto.spi.type.Type;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class WindowFunctionDefinition
{
    private final WindowFunctionSupplier functionSupplier;
    private final Type type;
    private final List<Integer> argumentChannels;

    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, Type type, List<Integer> inputs)
    {
        requireNonNull(functionSupplier, "functionSupplier is null");
        requireNonNull(type, "type is null");
        requireNonNull(inputs, "inputs is null");

        return new WindowFunctionDefinition(functionSupplier, type, inputs);
    }

    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, Type type, Integer... inputs)
    {
        requireNonNull(functionSupplier, "functionSupplier is null");
        requireNonNull(type, "type is null");
        requireNonNull(inputs, "inputs is null");

        return window(functionSupplier, type, Arrays.asList(inputs));
    }

    WindowFunctionDefinition(WindowFunctionSupplier functionSupplier, Type type, List<Integer> argumentChannels)
    {
        this.functionSupplier = functionSupplier;
        this.type = type;
        this.argumentChannels = argumentChannels;
    }

    public Type getType()
    {
        return type;
    }

    public WindowFunction createWindowFunction()
    {
        return functionSupplier.createWindowFunction(argumentChannels);
    }
}
