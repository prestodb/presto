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

import com.facebook.presto.operator.aggregation.state.LongAndDoubleState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("geometric_mean")
public final class FloatGeometricMeanAggregations
{
    private FloatGeometricMeanAggregations() {}

    @InputFunction
    public static void input(LongAndDoubleState state, @SqlType(StandardTypes.FLOAT) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + Math.log(intBitsToFloat((int) value)));
    }

    @CombineFunction
    public static void combine(LongAndDoubleState state, LongAndDoubleState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @OutputFunction(StandardTypes.FLOAT)
    public static void output(LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            FLOAT.writeLong(out, floatToRawIntBits((float) Math.exp(state.getDouble() / count)));
        }
    }
}
