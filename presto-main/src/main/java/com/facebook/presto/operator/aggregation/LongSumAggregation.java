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

import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class LongSumAggregation
        extends AbstractAggregationFunction<NullableLongState>
{
    public static final AggregationFunction LONG_SUM = new LongSumAggregation();

    public LongSumAggregation()
    {
        super(BIGINT, BIGINT, BIGINT);
    }

    @Override
    public void processInput(NullableLongState state, BlockCursor cursor)
    {
        state.setNull(false);
        state.setLong(state.getLong() + cursor.getLong());
    }

    @Override
    public void evaluateFinal(NullableLongState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            out.appendLong(state.getLong());
        }
    }
}
