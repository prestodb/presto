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
import com.google.common.primitives.Ints;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

@WindowFunctionSignature(name = "lag", typeVariable = "T", returnType = "T", argumentTypes = "T")
@WindowFunctionSignature(name = "lag", typeVariable = "T", returnType = "T", argumentTypes = {"T", "bigint"})
@WindowFunctionSignature(name = "lag", typeVariable = "T", returnType = "T", argumentTypes = {"T", "bigint", "T"})
public class LagFunction
        extends ValueWindowFunction
{
    private final int valueChannel;
    private final int offsetChannel;
    private final int defaultChannel;

    public LagFunction(List<Integer> argumentChannels)
    {
        this.valueChannel = argumentChannels.get(0);
        this.offsetChannel = (argumentChannels.size() > 1) ? argumentChannels.get(1) : -1;
        this.defaultChannel = (argumentChannels.size() > 2) ? argumentChannels.get(2) : -1;
    }

    @Override
    public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
    {
        if ((offsetChannel >= 0) && windowIndex.isNull(offsetChannel, currentPosition)) {
            output.appendNull();
        }
        else {
            long offset = (offsetChannel < 0) ? 1 : windowIndex.getLong(offsetChannel, currentPosition);
            checkCondition(offset >= 0, INVALID_FUNCTION_ARGUMENT, "Offset must be at least 0");

            long valuePosition = currentPosition - offset;

            if ((valuePosition >= 0) && (valuePosition <= currentPosition)) {
                windowIndex.appendTo(valueChannel, Ints.checkedCast(valuePosition), output);
            }
            else if (defaultChannel >= 0) {
                windowIndex.appendTo(defaultChannel, currentPosition, output);
            }
            else {
                output.appendNull();
            }
        }
    }
}
