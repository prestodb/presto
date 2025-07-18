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
import com.facebook.presto.operator.aggregation.state.ExtendedRegressionState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionAvgx;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionAvgy;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionCount;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionR2;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionSxx;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionSxy;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionSyy;
import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeExtendedRegressionState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateExtendedRegressionState;

@AggregationFunction
public class DoubleRegressionExtendedAggregation
{
    private DoubleRegressionExtendedAggregation() {}

    @InputFunction
    public static void input(@AggregationState ExtendedRegressionState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        updateExtendedRegressionState(state, independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(@AggregationState ExtendedRegressionState state, @AggregationState ExtendedRegressionState otherState)
    {
        mergeExtendedRegressionState(state, otherState);
    }

    @AggregationFunction("regr_sxy")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrSxy(@AggregationState ExtendedRegressionState state, BlockBuilder out)
    {
        double result = getRegressionSxy(state);
        double count = getRegressionCount(state);
        if (Double.isFinite(result) && Double.isFinite(count) && count > 0) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_sxx")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrSxx(@AggregationState ExtendedRegressionState state, BlockBuilder out)
    {
        double result = getRegressionSxx(state);
        double count = getRegressionCount(state);
        if (Double.isFinite(result) && Double.isFinite(count) && count > 0) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_syy")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrSyy(@AggregationState ExtendedRegressionState state, BlockBuilder out)
    {
        double result = getRegressionSyy(state);
        double count = getRegressionCount(state);
        if (Double.isFinite(result) && Double.isFinite(count) && count > 0) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_r2")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrR2(@AggregationState ExtendedRegressionState state, BlockBuilder out)
    {
        double result = getRegressionR2(state);
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_count")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrCount(@AggregationState ExtendedRegressionState state, BlockBuilder out)
    {
        double result = getRegressionCount(state);
        if (Double.isFinite(result) && result > 0) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_avgy")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrAvgy(@AggregationState ExtendedRegressionState state, BlockBuilder out)
    {
        double result = getRegressionAvgy(state);
        double count = getRegressionCount(state);
        if (Double.isFinite(result) && Double.isFinite(count) && count > 0) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_avgx")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrAvgx(@AggregationState ExtendedRegressionState state, BlockBuilder out)
    {
        double result = getRegressionAvgx(state);
        double count = getRegressionCount(state);
        if (Double.isFinite(result) && Double.isFinite(count) && count > 0) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }
}
