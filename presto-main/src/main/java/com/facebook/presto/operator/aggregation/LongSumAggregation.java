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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockInputFunction;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.BigintOperators;

import static com.facebook.presto.common.type.BigintType.BIGINT;

@AggregationFunction("sum")
public final class LongSumAggregation
{
    private LongSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState NullableLongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setNull(false);
        state.setLong(BigintOperators.add(state.getLong(), value));
    }

    @BlockInputFunction
    public static void sum(@AggregationState NullableLongState state, @BlockPosition Block value)
    {
        long sum = 0;
        boolean hasNonNull = false;
        int positions = value.getPositionCount();
        if (value.mayHaveNull()) {
            int index = 0;
            while (index < positions) {
                while (index < positions && !value.isNull(index)) {
                    sum += BIGINT.getLong(value, index);
                    index++;
                }
                if (index > 0) {
                    hasNonNull = true;
                }
                while (index < positions && value.isNull(index)) {
                    index++;
                }
            }
        }
        else {
            hasNonNull = true;
            for (int i = 0; i < positions; ++i) {
                sum += BIGINT.getLong(value, i);
            }
        }
        if (hasNonNull) {
            state.setNull(false);
            state.setLong(BigintOperators.add(state.getLong(), sum));
        }
    }

    @InputFunction
    public static void sum(@AggregationState NullableLongState state, @BlockPosition @SqlType("array(bigint)") Block value, @BlockIndex int index)
    {
        state.setNull(false);
        long sum = 0;
        Block block = value.getBlock(index);
        for (int i = 0; i < block.getPositionCount(); ++i) {
            sum += BIGINT.getLong(block, i);
        }
        state.setLong(BigintOperators.add(state.getLong(), sum));
    }

    @CombineFunction
    public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(otherState.getLong());
            return;
        }

        state.setLong(BigintOperators.add(state.getLong(), otherState.getLong()));
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(BIGINT, state, out);
    }
}
