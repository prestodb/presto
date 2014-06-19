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

import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.spi.block.Block;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class DoubleSumAggregation
        extends AbstractSimpleAggregationFunction<NullableDoubleState>
{
    public static final AggregationFunction DOUBLE_SUM = new DoubleSumAggregation();

    public DoubleSumAggregation()
    {
        super(DOUBLE, DOUBLE, DOUBLE);
    }

    @Override
    public void processInput(NullableDoubleState state, Block block, int index)
    {
        state.setNull(false);
        state.setDouble(state.getDouble() + block.getDouble(index));
    }
}
