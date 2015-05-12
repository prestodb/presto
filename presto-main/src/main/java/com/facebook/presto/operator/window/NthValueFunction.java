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
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.util.Failures.checkCondition;

public class NthValueFunction
        extends ValueWindowFunction
{
    public static class BigintNthValueFunction
            extends NthValueFunction
    {
        public BigintNthValueFunction(List<Integer> argumentChannels)
        {
            super(BIGINT, argumentChannels);
        }
    }

    public static class BooleanNthValueFunction
            extends NthValueFunction
    {
        public BooleanNthValueFunction(List<Integer> argumentChannels)
        {
            super(BOOLEAN, argumentChannels);
        }
    }

    public static class DoubleNthValueFunction
            extends NthValueFunction
    {
        public DoubleNthValueFunction(List<Integer> argumentChannels)
        {
            super(DOUBLE, argumentChannels);
        }
    }

    public static class VarcharNthValueFunction
            extends NthValueFunction
    {
        public VarcharNthValueFunction(List<Integer> argumentChannels)
        {
            super(VARCHAR, argumentChannels);
        }
    }

    public static class TimestampNthValueFunction
            extends NthValueFunction
    {
        public TimestampNthValueFunction(List<Integer> argumentChannels)
        {
            super(TIMESTAMP, argumentChannels);
        }
    }

    private final Type type;
    private final int valueChannel;
    private final int offsetChannel;

    protected NthValueFunction(Type type, List<Integer> argumentChannels)
    {
        this.type = type;
        this.valueChannel = argumentChannels.get(0);
        this.offsetChannel = argumentChannels.get(1);
    }

    @Override
    public Type getType()
    {
        return type;
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
                windowIndex.appendTo(valueChannel, Ints.checkedCast(valuePosition), output);
            }
            else {
                output.appendNull();
            }
        }
    }
}
