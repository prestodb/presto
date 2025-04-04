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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.BigintOperators;

import static com.facebook.presto.common.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;

@AggregationFunction("sum")
public final class IntervalYearToMonthSumAggregation
{
    private IntervalYearToMonthSumAggregation() {}

    @InputFunction
    public static void sum(NullableLongState state, @SqlType(INTERVAL_YEAR_TO_MONTH) long value)
    {
        state.setNull(false);
        state.setLong(BigintOperators.add(state.getLong(), value));
    }

    @CombineFunction
    public static void combine(NullableLongState state, NullableLongState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(otherState.getLong());
            return;
        }

        state.setLong(BigintOperators.add(state.getLong(), otherState.getLong()));
    }

    @OutputFunction(INTERVAL_YEAR_TO_MONTH)
    public static void output(NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(INTERVAL_YEAR_MONTH, state, out);
    }
}
