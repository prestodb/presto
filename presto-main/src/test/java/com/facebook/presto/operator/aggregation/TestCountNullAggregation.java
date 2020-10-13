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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class TestCountNullAggregation
        extends AbstractTestAggregationFunction
{
    @BeforeClass
    public void setup()
    {
        functionAndTypeManager.registerBuiltInFunctions(new FunctionListBuilder().aggregates(CountNull.class).getFunctions());
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return 0L;
    }

    @Override
    public Object getExpectedValueIncludingNulls(int start, int length, int lengthIncludingNulls)
    {
        return (long) lengthIncludingNulls - length;
    }

    @AggregationFunction("count_null")
    public static final class CountNull
    {
        private CountNull() {}

        @InputFunction
        public static void input(@AggregationState NullableLongState state, @BlockPosition @NullablePosition @SqlType(StandardTypes.BIGINT) Block block, @BlockIndex int position)
        {
            if (block.isNull(position)) {
                state.setLong(state.getLong() + 1);
            }
            state.setNull(false);
        }

        @CombineFunction
        public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState scratchState)
        {
            state.setLong(state.getLong() + scratchState.getLong());
            state.setNull(state.isNull() && scratchState.isNull());
        }

        @OutputFunction(StandardTypes.BIGINT)
        public static void output(@AggregationState NullableLongState state, BlockBuilder out)
        {
            NullableLongState.write(BIGINT, state, out);
        }
    }

    @Override
    protected String getFunctionName()
    {
        return "count_null";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.BIGINT);
    }
}
