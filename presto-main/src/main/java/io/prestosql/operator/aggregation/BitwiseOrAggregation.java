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
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.StandardTypes;

@AggregationFunction("bitwise_or_agg")
public class BitwiseOrAggregation
{
    private BitwiseOrAggregation() {}

    @InputFunction
    public static void bitOr(@AggregationState NullableLongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.isNull()) {
            state.setLong(value);
        }
        else {
            state.setLong(state.getLong() | value);
        }

        state.setNull(false);
    }

    @CombineFunction
    public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState otherState)
    {
        if (state.isNull()) {
            state.setNull(otherState.isNull());
            state.setLong(otherState.getLong());
        }
        else if (!otherState.isNull()) {
            state.setLong(state.getLong() | otherState.getLong());
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(BigintType.BIGINT, state, out);
    }
}
