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

import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeRegressionState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateRegressionState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("") // Names are on output methods
public class RegressionAggregation
{
    private RegressionAggregation() {}

    @InputFunction
    public static void input(RegressionState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        updateRegressionState(state, independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(RegressionState state, RegressionState otherState)
    {
        mergeRegressionState(state, otherState);
    }

    @AggregationFunction("regr_slope")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrSlope(RegressionState state, BlockBuilder out)
    {
        // Math comes from ISO9075-2:2011(E) 10.9 General Rules 7 c xii
        double dividend = state.getCount() * state.getSumXY() - state.getSumX() * state.getSumY();
        double divisor = state.getCount() * state.getSumXSquare() - state.getSumX() * state.getSumX();

        // divisor deliberately not checked for zero because the result can be Infty or NaN even if it is not zero
        double result = dividend / divisor;
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_intercept")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrIntercept(RegressionState state, BlockBuilder out)
    {
        // Math comes from ISO9075-2:2011(E) 10.9 General Rules 7 c xiii
        double dividend = state.getSumY() * state.getSumXSquare() - state.getSumX() * state.getSumXY();
        double divisor = state.getCount() * state.getSumXSquare() - state.getSumX() * state.getSumX();

        // divisor deliberately not checked for zero because the result can be Infty or NaN even if it is not zero
        double result = dividend / divisor;
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }
}
