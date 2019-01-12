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
package io.prestosql.operator.aggregation;

import io.prestosql.operator.aggregation.state.VarianceState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.aggregation.AggregationUtils.mergeVarianceState;
import static io.prestosql.operator.aggregation.AggregationUtils.updateVarianceState;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

@AggregationFunction
public final class VarianceAggregation
{
    private VarianceAggregation() {}

    @InputFunction
    public static void doubleInput(@AggregationState VarianceState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        updateVarianceState(state, value);
    }

    @InputFunction
    public static void bigintInput(@AggregationState VarianceState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        updateVarianceState(state, (double) value);
    }

    @CombineFunction
    public static void combine(@AggregationState VarianceState state, @AggregationState VarianceState otherState)
    {
        mergeVarianceState(state, otherState);
    }

    @AggregationFunction(value = "variance", alias = "var_samp")
    @Description("Returns the sample variance of the argument")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void variance(@AggregationState VarianceState state, BlockBuilder out)
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
    public static void variancePop(@AggregationState VarianceState state, BlockBuilder out)
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
    @Description("Returns the sample standard deviation of the argument")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void stddev(@AggregationState VarianceState state, BlockBuilder out)
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
    @Description("Returns the population standard deviation of the argument")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void stddevPop(@AggregationState VarianceState state, BlockBuilder out)
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
