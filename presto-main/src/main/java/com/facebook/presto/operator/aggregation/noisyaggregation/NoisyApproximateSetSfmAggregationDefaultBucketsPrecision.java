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
import com.facebook.presto.type.SfmSketchType;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.DEFAULT_BUCKET_COUNT;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.DEFAULT_PRECISION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.addHashToSketch;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.hashDouble;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.hashLong;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.hashSlice;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.mergeStates;
import static com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils.writeSketch;

@AggregationFunction(value = "noisy_approx_set_sfm")
public final class NoisyApproximateSetSfmAggregationDefaultBucketsPrecision
{
    private NoisyApproximateSetSfmAggregationDefaultBucketsPrecision() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState SfmSketchState state,
            @SqlType("T") long value,
            @SqlType(StandardTypes.DOUBLE) double epsilon)
    {
        addHashToSketch(state, hashLong(methodHandle, value), epsilon, DEFAULT_BUCKET_COUNT, DEFAULT_PRECISION);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState SfmSketchState state,
            @SqlType("T") double value,
            @SqlType(StandardTypes.DOUBLE) double epsilon)
    {
        addHashToSketch(state, hashDouble(methodHandle, value), epsilon, DEFAULT_BUCKET_COUNT, DEFAULT_PRECISION);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState SfmSketchState state,
            @SqlType("T") Slice value,
            @SqlType(StandardTypes.DOUBLE) double epsilon)
    {
        addHashToSketch(state, hashSlice(methodHandle, value), epsilon, DEFAULT_BUCKET_COUNT, DEFAULT_PRECISION);
    }

    @CombineFunction
    public static void combineState(@AggregationState SfmSketchState state, @AggregationState SfmSketchState otherState)
    {
        mergeStates(state, otherState);
    }

    @OutputFunction(SfmSketchType.NAME)
    public static void evaluateFinal(@AggregationState SfmSketchState state, BlockBuilder out)
    {
        writeSketch(state, out);
    }
}
