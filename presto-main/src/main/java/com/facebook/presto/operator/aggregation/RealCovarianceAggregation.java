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
import com.facebook.presto.operator.aggregation.state.CovarianceState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getCovariancePopulation;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getCovarianceSample;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction
public class RealCovarianceAggregation
{
    private RealCovarianceAggregation() {}

    @InputFunction
    public static void input(@AggregationState CovarianceState state, @SqlType(StandardTypes.REAL) long dependentValue, @SqlType(StandardTypes.REAL) long independentValue)
    {
        DoubleCovarianceAggregation.input(state, intBitsToFloat((int) dependentValue), intBitsToFloat((int) independentValue));
    }

    @CombineFunction
    public static void combine(@AggregationState CovarianceState state, @AggregationState CovarianceState otherState)
    {
        DoubleCovarianceAggregation.combine(state, otherState);
    }

    @AggregationFunction("covar_samp")
    @OutputFunction(StandardTypes.REAL)
    public static void covarSamp(@AggregationState CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() <= 1) {
            out.appendNull();
        }
        else {
            double result = getCovarianceSample(state);
            REAL.writeLong(out, Float.floatToRawIntBits((float) result));
        }
    }

    @AggregationFunction("covar_pop")
    @OutputFunction(StandardTypes.REAL)
    public static void covarPop(@AggregationState CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            double result = getCovariancePopulation(state);
            REAL.writeLong(out, Float.floatToRawIntBits((float) result));
        }
    }
}
