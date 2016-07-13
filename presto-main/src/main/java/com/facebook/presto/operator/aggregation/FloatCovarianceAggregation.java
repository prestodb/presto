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
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("")
public class FloatCovarianceAggregation
{
    private FloatCovarianceAggregation() {}

    @InputFunction
    public static void input(CovarianceState state, @SqlType(StandardTypes.FLOAT) long dependentValue, @SqlType(StandardTypes.FLOAT) long independentValue)
    {
        DoubleCovarianceAggregation.input(state, intBitsToFloat((int) dependentValue), intBitsToFloat((int) independentValue));
    }

    @CombineFunction
    public static void combine(CovarianceState state, CovarianceState otherState)
    {
        DoubleCovarianceAggregation.combine(state, otherState);
    }

    @AggregationFunction("covar_samp")
    @OutputFunction(StandardTypes.FLOAT)
    public static void covarSamp(CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() <= 1) {
            out.appendNull();
        }
        else {
            double result = getCovarianceSample(state);
            FLOAT.writeLong(out, Float.floatToRawIntBits((float) result));
        }
    }

    @AggregationFunction("covar_pop")
    @OutputFunction(StandardTypes.FLOAT)
    public static void covarPop(CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            double result = getCovariancePopulation(state);
            FLOAT.writeLong(out, Float.floatToRawIntBits((float) result));
        }
    }
}
