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

import com.facebook.presto.operator.aggregation.state.InitialDoubleValue;
import com.facebook.presto.operator.aggregation.state.LongAndDoubleState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("gavg")
public final class GeometricAverageAggregations
{
    public static final InternalAggregationFunction LONG_AVERAGE = new AggregationCompiler().generateAggregationFunction(GeometricAverageAggregations.class, DOUBLE, ImmutableList.<Type>of(BIGINT));
    public static final InternalAggregationFunction DOUBLE_AVERAGE = new AggregationCompiler().generateAggregationFunction(GeometricAverageAggregations.class, DOUBLE, ImmutableList.<Type>of(DOUBLE));

    private GeometricAverageAggregations() {}

    public static interface LongAndDoubleGeomState extends LongAndDoubleState
    {
        @InitialDoubleValue(value = 1.0d)
        double getDouble();
    }

    @InputFunction
    public static void input(LongAndDoubleGeomState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() * value);
    }

    @InputFunction
    public static void input(LongAndDoubleGeomState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() * value);
    }

    @CombineFunction
    public static void combine(LongAndDoubleGeomState state, LongAndDoubleGeomState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setDouble(state.getDouble() * otherState.getDouble());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(LongAndDoubleGeomState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double value = state.getDouble();
            double gavg = Math.pow(value, 1.0d / count);
            DOUBLE.writeDouble(out, gavg);
        }
    }
}
