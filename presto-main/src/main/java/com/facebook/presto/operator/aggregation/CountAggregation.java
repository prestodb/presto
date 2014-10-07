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

import com.facebook.presto.operator.aggregation.state.LongState;

@AggregationFunction("count")
public final class CountAggregation
{
    public static final InternalAggregationFunction COUNT = new AggregationCompiler().generateAggregationFunction(CountAggregation.class);

    private CountAggregation()
    {
    }

    @InputFunction
    public static void input(LongState state)
    {
        state.setLong(state.getLong() + 1);
    }

    @CombineFunction
    public static void combine(LongState state, LongState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
    }
}
