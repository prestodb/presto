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

import com.facebook.presto.operator.aggregation.state.TriStateBooleanState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.FALSE_VALUE;
import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.NULL_VALUE;
import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.TRUE_VALUE;

@AggregationFunction(value = "bool_or")
public final class BooleanOrAggregation
{
    private BooleanOrAggregation() {}

    @InputFunction
    public static void booleanOr(TriStateBooleanState state, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        // if value is true, the result is true
        if (value) {
            state.setByte(TRUE_VALUE);
        }
        else {
            // if the current value is unset, set result to false
            if (state.getByte() == NULL_VALUE) {
                state.setByte(FALSE_VALUE);
            }
        }
    }

    @CombineFunction
    public static void combine(TriStateBooleanState state, TriStateBooleanState otherState)
    {
        if (state.getByte() == NULL_VALUE) {
            state.setByte(otherState.getByte());
            return;
        }
        if (otherState.getByte() == TRUE_VALUE) {
            state.setByte(otherState.getByte());
        }
    }

    @OutputFunction(StandardTypes.BOOLEAN)
    public static void output(TriStateBooleanState state, BlockBuilder out)
    {
        TriStateBooleanState.write(BooleanType.BOOLEAN, state, out);
    }
}
