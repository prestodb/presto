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
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.TriStateBooleanState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.FALSE_VALUE;
import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.NULL_VALUE;
import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.TRUE_VALUE;

@AggregationFunction(value = "bool_and", alias = "every")
public final class BooleanAndAggregation
{
    private BooleanAndAggregation() {}

    @InputFunction
    public static void booleanAnd(@AggregationState TriStateBooleanState state, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        // if the value is false, the result is false
        if (!value) {
            state.setByte(FALSE_VALUE);
        }
        else {
            // if the current value is unset, set result to true
            if (state.getByte() == NULL_VALUE) {
                state.setByte(TRUE_VALUE);
            }
        }
    }

    @CombineFunction
    public static void combine(@AggregationState TriStateBooleanState state, @AggregationState TriStateBooleanState otherState)
    {
        if (state.getByte() == NULL_VALUE) {
            state.setByte(otherState.getByte());
            return;
        }
        if (otherState.getByte() == FALSE_VALUE) {
            state.setByte(FALSE_VALUE);
        }
    }

    @OutputFunction(StandardTypes.BOOLEAN)
    public static void output(@AggregationState TriStateBooleanState state, BlockBuilder out)
    {
        TriStateBooleanState.write(BooleanType.BOOLEAN, state, out);
    }
}
