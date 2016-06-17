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
import com.facebook.presto.spi.type.FloatType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.testing.AggregationTestUtils.generateInternalAggregationFunction;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("sum")
public final class FloatSumAggregation
{
    public static final InternalAggregationFunction FLOAT_SUM = generateInternalAggregationFunction(FloatSumAggregation.class);

    private FloatSumAggregation() {}

    @InputFunction
    public static void sum(NullableDoubleState state, @SqlType(StandardTypes.FLOAT) long value)
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

    @OutputFunction(StandardTypes.FLOAT)
    public static void output(NullableDoubleState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            FloatType.FLOAT.writeLong(out, floatToRawIntBits((float) state.getDouble()));
        }
    }
}
