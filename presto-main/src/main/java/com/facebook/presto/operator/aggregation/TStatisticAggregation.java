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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.CentralMomentsState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeCentralMomentsState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateCentralMomentsState;

@AggregationFunction
@Description("Returns the Student's t-statistic")
public final class TStatisticAggregation
{
    private TStatisticAggregation() {}

    @InputFunction
    public static void input(@AggregationState CentralMomentsState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        updateCentralMomentsState(state, value);
    }

    @InputFunction
    public static void input(@AggregationState CentralMomentsState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        updateCentralMomentsState(state, (double) value);
    }

    @CombineFunction
    public static void combine(@AggregationState CentralMomentsState state, @AggregationState CentralMomentsState otherState)
    {
        mergeCentralMomentsState(state, otherState);
    }

    @AggregationFunction(value = "t_statistic")
    @Description("Returns the Student's t-statistic for the one-sample t-test")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void t_statistic(@AggregationState CentralMomentsState state, BlockBuilder out)
    {
        long n = state.getCount();

        if (n < 2) {
            out.appendNull();
        }
        else {
            double sampStdDev = Math.sqrt(state.getM2() / (state.getCount() - 1));
            double result = state.getM1() / (sampStdDev / Math.sqrt(state.getCount()));
            DOUBLE.writeDouble(out, result);
        }
    }
}
