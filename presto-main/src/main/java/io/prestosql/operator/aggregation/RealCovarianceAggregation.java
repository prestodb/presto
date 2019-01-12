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

import io.prestosql.operator.aggregation.state.CovarianceState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.aggregation.AggregationUtils.getCovariancePopulation;
import static io.prestosql.operator.aggregation.AggregationUtils.getCovarianceSample;
import static io.prestosql.spi.type.RealType.REAL;
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
