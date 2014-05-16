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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

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
    public void evaluateIntermediate(LongAndDoubleState state, BlockBuilder output)
    {
        long count = state.getLong();
        double sum = state.getDouble();

        // TODO: replace this when general fixed with values are supported
        Slice value = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE);
        value.setLong(0, count);
        value.setDouble(SIZE_OF_LONG, sum);
        output.appendSlice(value);
    }

    @Override
    public void processIntermediate(LongAndDoubleState state, BlockCursor cursor)
    {
        Slice value = cursor.getSlice();
        long count = value.getLong(0);
        state.setLong(state.getLong() + count);

        double sum = value.getDouble(SIZE_OF_LONG);
        state.setDouble(state.getDouble() + sum);
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
