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

import com.facebook.presto.operator.aggregation.state.CovarianceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.operator.aggregation.AggregationUtils.getCovariancePopulation;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getCovarianceSample;
import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeCovarianceState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateCovarianceState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("")
public class DoubleCovarianceAggregation
{
    private DoubleCovarianceAggregation() {}

    @InputFunction
    public static void input(CovarianceState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        updateCovarianceState(state, independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(CovarianceState state, CovarianceState otherState)
    {
        mergeCovarianceState(state, otherState);
    }

    @AggregationFunction("covar_samp")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void covarSamp(CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() <= 1) {
            out.appendNull();
        }
        else {
            double result = getCovarianceSample(state);
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction("covar_pop")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void covarPop(CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            double result = getCovariancePopulation(state);
            DOUBLE.writeDouble(out, result);
        }
    }
}
