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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.aggregation.state.VarianceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeVarianceState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateVarianceState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("") // Names are on output methods
@Description("Returns the variance of the argument")
public final class VarianceAggregation
{
    private VarianceAggregation() {}

    @InputFunction
    public static void doubleInput(VarianceState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        updateVarianceState(state, value);
    }

    @InputFunction
    public static void bigintInput(VarianceState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        updateVarianceState(state, (double) value);
    }

    @CombineFunction
    public static void combine(VarianceState state, VarianceState otherState)
    {
        mergeVarianceState(state, otherState);
    }

    @AggregationFunction(value = "variance", alias = "var_samp")
    @Description("Returns the sample variance of the argument")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void variance(VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count < 2) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / (count - 1);
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction("var_pop")
    @Description("Returns the population variance of the argument")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void variancePop(VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / count;
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction(value = "stddev", alias = "stddev_samp")
    @OutputFunction(StandardTypes.DOUBLE)
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
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction("stddev_pop")
    @OutputFunction(StandardTypes.DOUBLE)
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
            DOUBLE.writeDouble(out, result);
        }
    }
}
