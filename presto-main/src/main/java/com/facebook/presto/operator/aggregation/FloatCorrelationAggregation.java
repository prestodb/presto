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

import static com.facebook.presto.operator.aggregation.AggregationUtils.getCorrelation;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("corr")
public class FloatCorrelationAggregation
{
    private FloatCorrelationAggregation() {}

    @InputFunction
    public static void input(CorrelationState state, @SqlType(StandardTypes.FLOAT) long dependentValue, @SqlType(StandardTypes.FLOAT) long independentValue)
    {
        DoubleCorrelationAggregation.input(state, intBitsToFloat((int) dependentValue), intBitsToFloat((int) independentValue));
    }

    @CombineFunction
    public static void combine(CorrelationState state, CorrelationState otherState)
    {
        DoubleCorrelationAggregation.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.FLOAT)
    public static void corr(CorrelationState state, BlockBuilder out)
    {
        double result = getCorrelation(state);
        if (Double.isFinite(result)) {
            long resultBits = floatToRawIntBits((float) Math.sqrt(result)); // sqrt cannot turn finite value to non-finite value
            FLOAT.writeLong(out, resultBits);
        }
        else {
            out.appendNull();
        }
    }
}
