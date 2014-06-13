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
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class AverageAggregation
        extends AbstractAggregationFunction<LongAndDoubleState>
{
    private final boolean inputIsLong;

    public AverageAggregation(Type parameterType)
    {
        super(DOUBLE, VARCHAR, parameterType);

        if (parameterType == BIGINT) {
            this.inputIsLong = true;
        }
        else if (parameterType == DOUBLE) {
            this.inputIsLong = false;
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT or DOUBLE, but was " + parameterType);
        }
    }

    @Override
    protected void processInput(LongAndDoubleState state, BlockCursor cursor)
    {
        state.setLong(state.getLong() + 1);

        double value;
        if (inputIsLong) {
            value = cursor.getLong();
        }
        else {
            value = cursor.getDouble();
        }
        state.setDouble(state.getDouble() + value);
    }

    @Override
    protected void combineState(LongAndDoubleState state, LongAndDoubleState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @Override
    protected void evaluateFinal(LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double value = state.getDouble();
            out.appendDouble(value / count);
        }
    }
}
