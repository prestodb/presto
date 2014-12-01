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

import com.facebook.presto.operator.aggregation.state.CorrelationState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.operator.aggregation.AggregationUtils.countBelowAggregationThreshold;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getCovariancePop;
import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeCorrelationState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateCorrelationState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("corr")
public class CorrelationAggregation
{
    private CorrelationAggregation() {}

    @InputFunction
    public static void input(CorrelationState state, @SqlType(StandardTypes.DOUBLE) double independentValue, @SqlType(StandardTypes.DOUBLE) double dependentValue)
    {
        updateCorrelationState(state, independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(CorrelationState state, CorrelationState otherState)
    {
        mergeCorrelationState(state, otherState);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void corr(CorrelationState state, BlockBuilder out)
    {
        if (countBelowAggregationThreshold(state)) {
            out.appendNull();
        }
        else {
            double corr = getCovariancePop(state) / (Math.sqrt(state.getDependentM2() * state.getIndependentM2()) / state.getCount());
            DOUBLE.writeDouble(out, corr);
        }
    }
}
