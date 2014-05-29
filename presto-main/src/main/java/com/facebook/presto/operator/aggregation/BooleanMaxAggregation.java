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

import com.facebook.presto.operator.aggregation.state.ByteState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class BooleanMaxAggregation
        extends AbstractAggregationFunction<ByteState>
{
    public static final BooleanMaxAggregation BOOLEAN_MAX = new BooleanMaxAggregation();

    private static final byte NULL_VALUE = 0;
    private static final byte TRUE_VALUE = 1;
    private static final byte FALSE_VALUE = -1;

    public BooleanMaxAggregation()
    {
        super(BOOLEAN, BOOLEAN, BOOLEAN);
    }

    @Override
    protected void processInput(ByteState state, BlockCursor cursor)
    {
        // if value is true, update the max to true
        if (cursor.getBoolean()) {
            state.setByte(TRUE_VALUE);
        }
        else {
            // if the current value is null, set the max to false
            if (state.getByte() == NULL_VALUE) {
                state.setByte(FALSE_VALUE);
            }
        }
    }

    @Override
    protected void evaluateFinal(ByteState state, BlockBuilder out)
    {
        if (state.getByte() == NULL_VALUE) {
            out.appendNull();
        }
        else {
            out.appendBoolean(state.getByte() == TRUE_VALUE);
        }
    }
}
