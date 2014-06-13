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

import com.facebook.presto.operator.aggregation.state.InitialDoubleValue;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.spi.block.BlockCursor;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class DoubleMinAggregation
        extends AbstractSimpleAggregationFunction<DoubleMinAggregation.DoubleMinState>
{
    public static final DoubleMinAggregation DOUBLE_MIN = new DoubleMinAggregation();

    public DoubleMinAggregation()
    {
        super(DOUBLE, DOUBLE, DOUBLE);
    }

    @Override
    public void processInput(DoubleMinState state, BlockCursor cursor)
    {
        state.setNull(false);
        state.setDouble(Math.min(state.getDouble(), cursor.getDouble()));
    }

    public interface DoubleMinState
            extends NullableDoubleState
    {
        @Override
        @InitialDoubleValue(Double.POSITIVE_INFINITY)
        double getDouble();
    }
}
