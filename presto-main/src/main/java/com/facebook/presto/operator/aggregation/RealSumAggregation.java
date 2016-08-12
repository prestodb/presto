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

import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.testing.AggregationTestUtils.generateInternalAggregationFunction;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("sum")
public final class RealSumAggregation
{
    public static final InternalAggregationFunction REAL_SUM = generateInternalAggregationFunction(RealSumAggregation.class);

    private RealSumAggregation() {}

    @InputFunction
    public static void sum(NullableDoubleState state, @SqlType(StandardTypes.REAL) long value)
    {
        state.setNull(false);
        state.setDouble(state.getDouble() + intBitsToFloat((int) value));
    }

    @CombineFunction
    public static void combine(NullableDoubleState state, NullableDoubleState otherState)
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
    public static void output(NullableDoubleState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToRawIntBits((float) state.getDouble()));
        }
    }
}
