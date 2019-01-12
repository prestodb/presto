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

import io.prestosql.operator.aggregation.state.TriStateBooleanState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.aggregation.state.TriStateBooleanState.FALSE_VALUE;
import static io.prestosql.operator.aggregation.state.TriStateBooleanState.NULL_VALUE;
import static io.prestosql.operator.aggregation.state.TriStateBooleanState.TRUE_VALUE;

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
