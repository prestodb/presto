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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionListBuilder;
import io.prestosql.operator.aggregation.state.NullableLongState;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.BeforeClass;

import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;

public class TestCountNullAggregation
        extends AbstractTestAggregationFunction
{
    @BeforeClass
    public void setup()
    {
        functionRegistry.addFunctions(new FunctionListBuilder().aggregates(CountNull.class).getFunctions());
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
