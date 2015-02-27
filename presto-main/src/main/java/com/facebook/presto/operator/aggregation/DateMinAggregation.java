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

import com.facebook.presto.operator.aggregation.state.NullableDateState;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.SqlType;

@AggregationFunction("min")
public final class DateMinAggregation
{
    private DateMinAggregation() {}

    @InputFunction
    @IntermediateInputFunction
    public static void max(DateMinState state, @SqlType(StandardTypes.DATE) long value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDate(value);
            return;
        }
        if (DateOperators.greaterThan(state.getDate(), value)) {
            state.setDate(value);
        }
    }

    public interface DateMinState
            extends NullableDateState
    {
        @Override
        long getDate();
    }
}
