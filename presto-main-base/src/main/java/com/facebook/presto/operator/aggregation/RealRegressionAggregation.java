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
import com.facebook.presto.operator.aggregation.state.RegressionState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionIntercept;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getRegressionSlope;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction
public class RealRegressionAggregation
{
    private RealRegressionAggregation() {}

    @InputFunction
    public static void input(@AggregationState RegressionState state, @SqlType(StandardTypes.REAL) long dependentValue, @SqlType(StandardTypes.REAL) long independentValue)
    {
        DoubleRegressionAggregation.input(state, intBitsToFloat((int) dependentValue), intBitsToFloat((int) independentValue));
    }

    @CombineFunction
    public static void combine(@AggregationState RegressionState state, @AggregationState RegressionState otherState)
    {
        DoubleRegressionAggregation.combine(state, otherState);
    }

    @AggregationFunction("regr_slope")
    @OutputFunction(StandardTypes.REAL)
    public static void regrSlope(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = getRegressionSlope(state);
        if (Double.isFinite(result)) {
            REAL.writeLong(out, floatToRawIntBits((float) result));
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_intercept")
    @OutputFunction(StandardTypes.REAL)
    public static void regrIntercept(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = getRegressionIntercept(state);
        if (Double.isFinite(result)) {
            REAL.writeLong(out, floatToRawIntBits((float) result));
        }
        else {
            out.appendNull();
        }
    }
}
