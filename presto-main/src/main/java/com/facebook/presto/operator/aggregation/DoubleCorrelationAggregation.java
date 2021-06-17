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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.AggregationFunction;
import com.facebook.presto.common.function.AggregationState;
import com.facebook.presto.common.function.CombineFunction;
import com.facebook.presto.common.function.InputFunction;
import com.facebook.presto.common.function.OutputFunction;
import com.facebook.presto.common.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.CorrelationState;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getCorrelation;
import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeCorrelationState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateCorrelationState;

@AggregationFunction("corr")
public class DoubleCorrelationAggregation
{
    private DoubleCorrelationAggregation() {}

    @InputFunction
    public static void input(@AggregationState CorrelationState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        updateCorrelationState(state, independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(@AggregationState CorrelationState state, @AggregationState CorrelationState otherState)
    {
        mergeCorrelationState(state, otherState);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void corr(@AggregationState CorrelationState state, BlockBuilder out)
    {
        double result = getCorrelation(state);
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }
}
