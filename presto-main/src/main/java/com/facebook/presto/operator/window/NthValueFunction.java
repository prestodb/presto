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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.ValueWindowFunction;
import com.facebook.presto.spi.function.WindowFunctionSignature;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

@WindowFunctionSignature(name = "nth_value", typeVariable = "T", returnType = "T", argumentTypes = {"T", "bigint"})
public class NthValueFunction
        extends ValueWindowFunction
{
    private final int valueChannel;
    private final int offsetChannel;
    private final boolean ignoreNulls;

    public NthValueFunction(List<Integer> argumentChannels, boolean ignoreNulls)
    {
        this.valueChannel = argumentChannels.get(0);
        this.offsetChannel = argumentChannels.get(1);
        this.ignoreNulls = ignoreNulls;
    }

    @Override
    public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
    {
        if ((frameStart < 0) || windowIndex.isNull(offsetChannel, currentPosition)) {
            output.appendNull();
        }
        else {
            long offset = windowIndex.getLong(offsetChannel, currentPosition);
            checkCondition(offset >= 1, INVALID_FUNCTION_ARGUMENT, "Offset must be at least 1");

            // offset is base 1
            long valuePosition = frameStart + (offset - 1);

            if ((valuePosition >= frameStart) && (valuePosition <= frameEnd)) {
                if (ignoreNulls) {
                    while (valuePosition >= 0 && valuePosition <= frameEnd) {
                        if (!windowIndex.isNull(valueChannel, (int) valuePosition)) {
                            break;
                        }

                        valuePosition++;
                    }

                    if (valuePosition > frameEnd) {
                        output.appendNull();
                        return;
                    }
                }

                windowIndex.appendTo(valueChannel, toIntExact(valuePosition), output);
            }
            else {
                output.appendNull();
            }
        }
    }
}
