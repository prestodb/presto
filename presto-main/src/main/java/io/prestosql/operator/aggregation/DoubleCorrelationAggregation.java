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

import io.prestosql.operator.aggregation.state.CorrelationState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.aggregation.AggregationUtils.getCorrelation;
import static io.prestosql.operator.aggregation.AggregationUtils.mergeCorrelationState;
import static io.prestosql.operator.aggregation.AggregationUtils.updateCorrelationState;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

@AggregationFunction("corr")
public class DoubleCorrelationAggregation
{
    private DoubleCorrelationAggregation() {}

    @InputFunction
    public static void input(@AggregationState CorrelationState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        updateCorrelationState(state, independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(@AggregationState CorrelationState state, @AggregationState CorrelationState otherState)
    {
        mergeCorrelationState(state, otherState);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void corr(@AggregationState CorrelationState state, BlockBuilder out)
    {
        double result = getCorrelation(state);
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }
}
