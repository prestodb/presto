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

import com.facebook.presto.operator.aggregation.state.VarianceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeVarianceState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateVarianceState;

@AggregationFunction("") // Names are on output methods
public final class VarianceAggregation
{
    private VarianceAggregation() {}

    @InputFunction
    public static void doubleInput(VarianceState state, @SqlType(DoubleType.class) double value)
    {
        updateVarianceState(state, value);
    }

    @InputFunction
    public static void bigintInput(VarianceState state, @SqlType(BigintType.class) long value)
    {
        updateVarianceState(state, (double) value);
    }

    @CombineFunction
    public static void combine(VarianceState state, VarianceState otherState)
    {
        mergeVarianceState(state, otherState);
    }

    @AggregationFunction("var_samp")
    @OutputFunction(DoubleType.class)
    public static void varianceSamp(VarianceState state, BlockBuilder out)
    {
        variance(state, out);
    }

    @AggregationFunction("variance")
    @OutputFunction(DoubleType.class)
    public static void variance(VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count < 2) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / (count - 1);
            out.appendDouble(result);
        }
    }

    @AggregationFunction("var_pop")
    @OutputFunction(DoubleType.class)
    public static void variancePop(VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / count;
            out.appendDouble(result);
        }
    }

    @AggregationFunction("stddev")
    @OutputFunction(DoubleType.class)
    public static void stddevSamp(VarianceState state, BlockBuilder out)
    {
        stddev(state, out);
    }

    @AggregationFunction("stddev_samp")
    @OutputFunction(DoubleType.class)
    public static void stddev(VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count < 2) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / (count - 1);
            result = Math.sqrt(result);
            out.appendDouble(result);
        }
    }

    @AggregationFunction("stddev_pop")
    @OutputFunction(DoubleType.class)
    public static void stddevPop(VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / count;
            result = Math.sqrt(result);
            out.appendDouble(result);
        }
    }
}
