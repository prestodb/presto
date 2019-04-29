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

import com.facebook.presto.operator.aggregation.state.EntropyState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("entropy")
public final class EntropyAggregation
{
    @InputFunction
    public static void input(
            @AggregationState EntropyState state,
            @SqlNullable @SqlType(StandardTypes.BIGINT) long count)
    {
        /*if (count == null) {
            return;
        }*/

        if (count == 0) {
            return;
        }
        if (count < 0) {
            state.setNull(true);
            return;
        }

        final double countVal = count;
        state.setSumC(state.getSumC() + countVal);
        state.setSumCLogC(state.getSumC() + countVal * Math.log(countVal));
    }

    @CombineFunction
    public static void combine(@AggregationState EntropyState state, @AggregationState EntropyState otherState)
    {
        state.setSumC(state.getSumC() + otherState.getSumC());
        state.setSumCLogC(state.getSumCLogC() + otherState.getSumCLogC());
        state.setNull(state.getNull() || otherState.getNull());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState EntropyState state, BlockBuilder out)
    {
        if (state.getNull()) {
            out.appendNull();
            return;
        }
        if (state.getSumC() == 0) {
            DOUBLE.writeDouble(out, 0);
            return;
        }
        double entropy = Math.max(
                (-state.getSumCLogC() / state.getSumC() + Math.log(state.getSumC())) / Math.log(2),
                0);
        entropy = Math.max(entropy, 0);
        DOUBLE.writeDouble(out, entropy);
    }

    private EntropyAggregation() {}
}
