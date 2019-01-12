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

import io.prestosql.operator.aggregation.state.NullableLongState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.type.BigintOperators;

import static io.prestosql.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.prestosql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;

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
