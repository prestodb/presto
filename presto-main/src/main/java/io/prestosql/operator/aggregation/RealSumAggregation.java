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

import io.prestosql.operator.aggregation.state.NullableDoubleState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("sum")
public final class RealSumAggregation
{
    private RealSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState NullableDoubleState state, @SqlType(StandardTypes.REAL) long value)
    {
        state.setNull(false);
        state.setDouble(state.getDouble() + intBitsToFloat((int) value));
    }

    @CombineFunction
    public static void combine(@AggregationState NullableDoubleState state, @AggregationState NullableDoubleState otherState)
    {
        if (state.isNull()) {
            if (otherState.isNull()) {
                return;
            }
            state.setNull(false);
            state.setDouble(otherState.getDouble());
            return;
        }

        if (!otherState.isNull()) {
            state.setDouble(state.getDouble() + otherState.getDouble());
        }
    }

    @OutputFunction(StandardTypes.REAL)
    public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToRawIntBits((float) state.getDouble()));
        }
    }
}
