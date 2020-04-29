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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.EntropyState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 *  Calculates the log-2 entropy of count inputs.
 *
 *  Given counts $c_1, c_2, ..., cn$ ($c_i \geq 0)$, this aggregation calculates $\sum_i [ p_i log_2(-p_i) ]$,
 *      where $p_i = {c_i \over \sum_i [ c_i ]}$. If $\sum_i [ c_i ] = 0$, the entropy is defined to be 0.
 */
@AggregationFunction("entropy")
@Description("Takes non-negative count inputs, and computes the log-2 entropy of their fractions when normalized to sum to 1.")
public final class EntropyAggregation
{
    private EntropyAggregation() {}

    /**
     * @note If count is negative, the value of the aggregation will be null; if
     * count is 0, this is a no-op (since, in the context of entropy, 0 log(0) = 0; if count is null,
     * this is a no op.
     */
    @InputFunction
    public static void input(
            @AggregationState EntropyState state,
            @SqlType(StandardTypes.BIGINT) long count)
    {
        if (count < 0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Entropy count argument must be non-negative");
        }

        if (count == 0) {
            return;
        }
        state.setSumC(state.getSumC() + count);
        state.setSumCLogC(state.getSumCLogC() + count * Math.log(count));
    }

    @CombineFunction
    public static void combine(@AggregationState EntropyState state, @AggregationState EntropyState otherState)
    {
        state.setSumC(state.getSumC() + otherState.getSumC());
        state.setSumCLogC(state.getSumCLogC() + otherState.getSumCLogC());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState EntropyState state, BlockBuilder out)
    {
        Double entropy = 0.0;
        if (state.getSumC() > 0.0) {
            entropy = Math.max(
                    (Math.log(state.getSumC()) - state.getSumCLogC() / state.getSumC()) / Math.log(2.0),
                    0.0);
        }

        DOUBLE.writeDouble(out, entropy);
    }
}
