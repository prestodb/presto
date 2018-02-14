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

import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.BigintOperators;

import static com.facebook.presto.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;

@AggregationFunction("sum")
public final class IntervalDayToSecondSumAggregation
{
    private IntervalDayToSecondSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState NullableLongState state, @SqlType(INTERVAL_DAY_TO_SECOND) long value)
    {
        state.setNull(false);
        state.setLong(BigintOperators.add(state.getLong(), value));
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

    @OutputFunction(INTERVAL_DAY_TO_SECOND)
    public static void output(@AggregationState NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(INTERVAL_DAY_TIME, state, out);
    }
}
