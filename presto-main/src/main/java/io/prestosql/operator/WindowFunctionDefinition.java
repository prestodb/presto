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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.function.WindowFunction;
import io.prestosql.spi.type.Type;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class WindowFunctionDefinition
{
    private final WindowFunctionSupplier functionSupplier;
    private final Type type;
    private final FrameInfo frameInfo;
    private final List<Integer> argumentChannels;

    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, Type type, FrameInfo frameInfo, List<Integer> inputs)
    {
        return new WindowFunctionDefinition(functionSupplier, type, frameInfo, inputs);
    }

    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, Type type, FrameInfo frameInfo, Integer... inputs)
    {
        return window(functionSupplier, type, frameInfo, Arrays.asList(inputs));
    }

    WindowFunctionDefinition(WindowFunctionSupplier functionSupplier, Type type, FrameInfo frameInfo, List<Integer> argumentChannels)
    {
        requireNonNull(functionSupplier, "functionSupplier is null");
        requireNonNull(type, "type is null");
        requireNonNull(frameInfo, "frameInfo is null");
        requireNonNull(argumentChannels, "inputs is null");

        this.functionSupplier = functionSupplier;
        this.type = type;
        this.frameInfo = frameInfo;
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
    }

    public FrameInfo getFrameInfo()
    {
        return frameInfo;
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
