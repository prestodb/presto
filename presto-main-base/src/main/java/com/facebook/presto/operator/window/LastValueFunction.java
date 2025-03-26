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
package com.facebook.presto.operator.window;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.ValueWindowFunction;
import com.facebook.presto.spi.function.WindowFunctionSignature;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

@WindowFunctionSignature(name = "last_value", typeVariable = "T", returnType = "T", argumentTypes = "T")
public class LastValueFunction
        extends ValueWindowFunction
{
    private final int argumentChannel;

    public LastValueFunction(List<Integer> argumentChannels)
    {
        this.argumentChannel = getOnlyElement(argumentChannels);
    }

    @Override
    public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
    {
        if (frameStart < 0) {
            output.appendNull();
            return;
        }

        int valuePosition = frameEnd;

        if (ignoreNulls) {
            while (valuePosition >= frameStart) {
                if (!windowIndex.isNull(argumentChannel, valuePosition)) {
                    break;
                }

                valuePosition--;
            }

            if (valuePosition < frameStart) {
                output.appendNull();
                return;
            }
        }

        windowIndex.appendTo(argumentChannel, valuePosition, output);
    }
}
