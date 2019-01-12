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

import io.prestosql.operator.aggregation.state.DoubleHistogramStateSerializer;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateMetadata;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.DoubleType;

import java.util.Map;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

@AggregationFunction("numeric_histogram")
public final class DoubleHistogramAggregation
{
    public static final int ENTRY_BUFFER_SIZE = 100;

    private DoubleHistogramAggregation()
    {
    }

    @AccumulatorStateMetadata(stateSerializerClass = DoubleHistogramStateSerializer.class, stateFactoryClass = NumericHistogramStateFactory.class)
    public interface State
            extends AccumulatorState
    {
        NumericHistogram get();

        void set(NumericHistogram value);
    }

    @InputFunction
    public static void add(@AggregationState State state, @SqlType(BIGINT) long buckets, @SqlType(DOUBLE) double value, @SqlType(DOUBLE) double weight)
    {
        NumericHistogram histogram = state.get();
        if (histogram == null) {
            checkCondition(buckets >= 2, INVALID_FUNCTION_ARGUMENT, "numeric_histogram bucket count must be greater than one");
            histogram = new NumericHistogram(toIntExact(buckets), ENTRY_BUFFER_SIZE);
            state.set(histogram);
        }

        histogram.add(value, weight);
    }

    @InputFunction
    public static void add(@AggregationState State state, @SqlType(BIGINT) long buckets, @SqlType(DOUBLE) double value)
    {
        add(state, buckets, value, 1);
    }

    @CombineFunction
    public static void merge(@AggregationState State state, State other)
    {
        NumericHistogram input = other.get();
        NumericHistogram previous = state.get();

        if (previous == null) {
            state.set(input);
        }
        else {
            previous.mergeWith(input);
        }
    }

    @OutputFunction("map(double,double)")
    public static void output(@AggregationState State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            Map<Double, Double> value = state.get().getBuckets();

            BlockBuilder entryBuilder = out.beginBlockEntry();
            for (Map.Entry<Double, Double> entry : value.entrySet()) {
                DoubleType.DOUBLE.writeDouble(entryBuilder, entry.getKey());
                DoubleType.DOUBLE.writeDouble(entryBuilder, entry.getValue());
            }
            out.closeEntry();
        }
    }
}
