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

import com.facebook.presto.operator.aggregation.state.RegressionState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Function;

import javax.annotation.Nullable;

import static com.facebook.presto.operator.aggregation.AggregationUtils.countBelowAggregationThreshold;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getCovariancePop;
import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeRegularSlopeState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateRegressionState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("")
public class RegressionAggregation
{
    private static Function<RegressionState, Double> regrSlope = new Function<RegressionState, Double>()
    {
        @Nullable
        @Override
        public Double apply(@Nullable RegressionState state)
        {
            return getCovariancePop(state) / (state.getDependentM2() / state.getCount());
        }
    };

    private static Function<RegressionState, Double> regrIntercept = new Function<RegressionState, Double>()
    {
        @Nullable
        @Override
        public Double apply(@Nullable RegressionState state)
        {
            return state.getIndependentSum() / state.getCount() - regrSlope.apply(state) * (state.getDependentSum() / state.getCount());
        }
    };

    private RegressionAggregation() {}

    @InputFunction
    public static void input(RegressionState state, @SqlType(StandardTypes.DOUBLE) double independentValue, @SqlType(StandardTypes.DOUBLE) double dependentValue)
    {
        updateRegressionState(state, independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(RegressionState state, RegressionState otherState)
    {
        mergeRegularSlopeState(state, otherState);
    }

    @AggregationFunction("regr_slope")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void corr(RegressionState state, BlockBuilder out)
    {
        appendResultToBlockBuilder(state, out, regrSlope);
    }

    @AggregationFunction("regr_intercept")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrIntercept(RegressionState state, BlockBuilder out)
    {
        appendResultToBlockBuilder(state, out, regrIntercept);
    }

    private static void appendResultToBlockBuilder(RegressionState state, BlockBuilder out, Function<RegressionState, Double> func)
    {
        if (countBelowAggregationThreshold(state)) {
            out.appendNull();
        }
        else {
            DOUBLE.writeDouble(out, func.apply(state));
        }
    }
}
