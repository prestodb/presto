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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

@AggregationFunction("min")
public final class DoubleMinAggregation
{
    private DoubleMinAggregation() {}

    @InputFunction
    @IntermediateInputFunction
    public static void min(DoubleMinState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setNull(false);
        state.setDouble(Math.min(state.getDouble(), value));
    }

    public interface DoubleMinState
            extends NullableDoubleState
    {
        @Override
        @InitialDoubleValue(Double.POSITIVE_INFINITY)
        double getDouble();
    }
}
