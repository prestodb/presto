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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.addHashToSketch;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.hashDouble;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.hashLong;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.hashSlice;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.mergeStates;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.writeCardinality;

@AggregationFunction(value = "noisy_approx_distinct_sfm")
public final class NoisyApproximateDistinctCountSfmAggregation
{
    private NoisyApproximateDistinctCountSfmAggregation() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState SfmSketchState state,
            @SqlType("T") long value,
            @SqlType(StandardTypes.DOUBLE) double epsilon,
            @SqlType(StandardTypes.BIGINT) long numberOfBuckets,
            @SqlType(StandardTypes.BIGINT) long precision)
    {
        addHashToSketch(state, hashLong(methodHandle, value), epsilon, numberOfBuckets, precision);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState SfmSketchState state,
            @SqlType("T") double value,
            @SqlType(StandardTypes.DOUBLE) double epsilon,
            @SqlType(StandardTypes.BIGINT) long numberOfBuckets,
            @SqlType(StandardTypes.BIGINT) long precision)
    {
        addHashToSketch(state, hashDouble(methodHandle, value), epsilon, numberOfBuckets, precision);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState SfmSketchState state,
            @SqlType("T") Slice value,
            @SqlType(StandardTypes.DOUBLE) double epsilon,
            @SqlType(StandardTypes.BIGINT) long numberOfBuckets,
            @SqlType(StandardTypes.BIGINT) long precision)
    {
        addHashToSketch(state, hashSlice(methodHandle, value), epsilon, numberOfBuckets, precision);
    }

    @CombineFunction
    public static void combineState(@AggregationState SfmSketchState state, @AggregationState SfmSketchState otherState)
    {
        mergeStates(state, otherState);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(@AggregationState SfmSketchState state, BlockBuilder out)
    {
        writeCardinality(state, out);
    }
}
