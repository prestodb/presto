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
import com.facebook.presto.spi.block.Block;

import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.FALSE_VALUE;
import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.NULL_VALUE;
import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.TRUE_VALUE;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class BooleanMaxAggregation
        extends AbstractSimpleAggregationFunction<TriStateBooleanState>
{
    public static final BooleanMaxAggregation BOOLEAN_MAX = new BooleanMaxAggregation();

    public BooleanMaxAggregation()
    {
        super(BOOLEAN, BOOLEAN, BOOLEAN);
    }

    @Override
    protected void processInput(TriStateBooleanState state, Block block, int index)
    {
        // if value is true, update the max to true
        if (block.getBoolean(index)) {
            state.setByte(TRUE_VALUE);
        }
        else {
            // if the current value is null, set the max to false
            if (state.getByte() == NULL_VALUE) {
                state.setByte(FALSE_VALUE);
            }
        }
    }
}
