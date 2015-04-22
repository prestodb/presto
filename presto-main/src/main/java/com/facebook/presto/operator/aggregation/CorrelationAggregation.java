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

import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeCorrelationState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateCorrelationState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("corr")
public class CorrelationAggregation
{
    private CorrelationAggregation() {}

    @InputFunction
    public static void input(CorrelationState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
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
        // Math comes from ISO9075-2:2011(E) 10.9 General Rules 7 c x
        double dividend = state.getCount() * state.getSumXY() - state.getSumX() * state.getSumY();
        dividend = dividend * dividend;
        double divisor1 = state.getCount() * state.getSumXSquare() - state.getSumX() * state.getSumX();
        double divisor2 = state.getCount() * state.getSumYSquare() - state.getSumY() * state.getSumY();

        // divisor1 and divisor2 deliberately not checked for zero because the result can be Infty or NaN even if they are both not zero
        double result = dividend / divisor1 / divisor2; // When the left expression yields a finite value, dividend / (divisor1 * divisor2) can yield Infty or NaN.
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, Math.sqrt(result)); // sqrt cannot turn finite value to non-finite value
        }
        else {
            out.appendNull();
        }
    }
}
