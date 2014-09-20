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

import com.facebook.presto.operator.aggregation.state.InitialLongValue;
import com.facebook.presto.operator.aggregation.state.NullableBigintState;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

@AggregationFunction("min")
public final class LongMinAggregation
{
    private LongMinAggregation() {}

    @InputFunction
    @IntermediateInputFunction
    public static void min(BigintMinState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setNull(false);
        state.setLong(Math.min(state.getLong(), value));
    }

    public interface BigintMinState
            extends NullableBigintState
    {
        @Override
        @InitialLongValue(Long.MAX_VALUE)
        long getLong();
    }
}
