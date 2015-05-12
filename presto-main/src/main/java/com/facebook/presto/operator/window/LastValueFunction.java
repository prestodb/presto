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

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.collect.Iterables.getOnlyElement;

public class LastValueFunction
        extends ValueWindowFunction
{
    public static class BigintLastValueFunction
            extends LastValueFunction
    {
        public BigintLastValueFunction(List<Integer> argumentChannels)
        {
            super(BIGINT, argumentChannels);
        }
    }

    public static class BooleanLastValueFunction
            extends LastValueFunction
    {
        public BooleanLastValueFunction(List<Integer> argumentChannels)
        {
            super(BOOLEAN, argumentChannels);
        }
    }

    public static class DoubleLastValueFunction
            extends LastValueFunction
    {
        public DoubleLastValueFunction(List<Integer> argumentChannels)
        {
            super(DOUBLE, argumentChannels);
        }
    }

    public static class VarcharLastValueFunction
            extends LastValueFunction
    {
        public VarcharLastValueFunction(List<Integer> argumentChannels)
        {
            super(VARCHAR, argumentChannels);
        }
    }

    public static class TimestampLastValueFunction
            extends LastValueFunction
    {
        public TimestampLastValueFunction(List<Integer> argumentChannels)
        {
            super(TIMESTAMP, argumentChannels);
        }
    }

    private final Type type;
    private final int argumentChannel;

    protected LastValueFunction(Type type, List<Integer> argumentChannels)
    {
        this.type = type;
        this.argumentChannel = getOnlyElement(argumentChannels);
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
    {
        if (frameStart < 0) {
            output.appendNull();
            return;
        }

        windowIndex.appendTo(argumentChannel, frameEnd, output);
    }
}
