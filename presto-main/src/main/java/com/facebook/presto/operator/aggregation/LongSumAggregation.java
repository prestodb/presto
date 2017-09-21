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

import com.facebook.presto.operator.aggregation.state.LongLongState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.RemoveInputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.BigintOperators;

@AggregationFunction("sum")
public final class LongSumAggregation
{
    private LongSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState LongLongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setFirst(state.getFirst() + 1);
        state.setSecond(BigintOperators.add(state.getSecond(), value));
    }

    @RemoveInputFunction
    public static void removeInput(@AggregationState LongLongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setFirst(state.getFirst() - 1);
        state.setSecond(BigintOperators.subtract(state.getSecond(), value));
    }

    @CombineFunction
    public static void combine(@AggregationState LongLongState state, @AggregationState LongLongState otherState)
    {
        state.setFirst(state.getFirst() + otherState.getFirst());
        state.setSecond(BigintOperators.add(state.getSecond(), otherState.getSecond()));
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState LongLongState state, BlockBuilder out)
    {
        if (state.getFirst() == 0) {
            out.appendNull();
        }
        else {
            BigintType.BIGINT.writeLong(out, state.getSecond());
        }
    }
}
