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

import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.sql.tree.Input;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class WindowFunctionDefinition
{
    private final WindowFunctionSupplier functionSupplier;
    private final List<Integer> argumentChannels;

    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, List<Input> inputs)
    {
        Preconditions.checkNotNull(functionSupplier, "functionSupplier is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        return new WindowFunctionDefinition(functionSupplier, Lists.transform(inputs, Input.channelGetter()));
    }

    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, Input... inputs)
    {
        Preconditions.checkNotNull(functionSupplier, "functionSupplier is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        return window(functionSupplier, Arrays.asList(inputs));
    }

    WindowFunctionDefinition(WindowFunctionSupplier functionSupplier, List<Integer> argumentChannels)
    {
        this.functionSupplier = functionSupplier;
        this.argumentChannels = argumentChannels;
    }

    public WindowFunction createWindowFunction()
    {
        return functionSupplier.createWindowFunction(argumentChannels);
    }
}
