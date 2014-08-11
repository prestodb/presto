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

import com.facebook.presto.operator.aggregation.state.NullableBigintState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class TestCountNullAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus());
        for (int i = start; i < start + length; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        return blockBuilder.build();
    }

    @Override
    public InternalAggregationFunction getFunction()
    {
        return new AggregationCompiler().generateAggregationFunction(CountNull.class);
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
        public static void input(NullableBigintState state, @NullablePosition @SqlType(BigintType.class) Block block, @BlockIndex int position)
        {
            if (block.isNull(position)) {
                state.setLong(state.getLong() + 1);
            }
            state.setNull(false);
        }

        @CombineFunction
        public static void combine(NullableBigintState state, NullableBigintState scratchState)
        {
            state.setLong(state.getLong() + scratchState.getLong());
            state.setNull(state.isNull() && scratchState.isNull());
        }
    }
}
