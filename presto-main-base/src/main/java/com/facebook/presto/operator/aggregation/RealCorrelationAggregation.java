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
import com.facebook.presto.operator.aggregation.state.CorrelationState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getCorrelation;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("corr")
public class RealCorrelationAggregation
{
    private RealCorrelationAggregation() {}

    @InputFunction
    public static void input(@AggregationState CorrelationState state, @SqlType(StandardTypes.REAL) long dependentValue, @SqlType(StandardTypes.REAL) long independentValue)
    {
        DoubleCorrelationAggregation.input(state, intBitsToFloat((int) dependentValue), intBitsToFloat((int) independentValue));
    }

    @CombineFunction
    public static void combine(@AggregationState CorrelationState state, @AggregationState CorrelationState otherState)
    {
        DoubleCorrelationAggregation.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.REAL)
    public static void corr(@AggregationState CorrelationState state, BlockBuilder out)
    {
        double result = getCorrelation(state);
        if (Double.isFinite(result)) {
            long resultBits = floatToRawIntBits((float) result);
            REAL.writeLong(out, resultBits);
        }
        else {
            out.appendNull();
        }
    }
}
