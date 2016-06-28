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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.testing.AggregationTestUtils.generateInternalAggregationFunction;

@AggregationFunction("sum")
public final class DoubleSumAggregation
{
    public static final InternalAggregationFunction DOUBLE_SUM = generateInternalAggregationFunction(DoubleSumAggregation.class);

    private DoubleSumAggregation() {}

    @InputFunction
    @IntermediateInputFunction
    public static void sum(NullableDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setNull(false);
        state.setDouble(state.getDouble() + value);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(NullableDoubleState state, BlockBuilder out)
    {
        NullableDoubleState.write(DoubleType.DOUBLE, state, out);
    }
}
