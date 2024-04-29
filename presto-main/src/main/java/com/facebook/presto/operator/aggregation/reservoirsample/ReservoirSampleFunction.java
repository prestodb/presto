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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.NullablePosition;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;

@AggregationFunction(value = "reservoir_sample", isCalledOnNullInput = true)
@Description("Generates a fixed-size bernoulli sample from the input column. Will merge an existing sample into the newly-generated sample.")
public class ReservoirSampleFunction
{
    public static final String NAME = "reservoir_sample";

    private ReservoirSampleFunction()
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @TypeParameter("T") Type type,
            @AggregationState ReservoirSampleState state,
            @BlockPosition @SqlType("array(T)") @NullablePosition Block initialState,
            @BlockIndex int initialStatePos,
            @SqlType(BIGINT) long initialProcessedCount,
            @BlockPosition @SqlType("T") @NullablePosition Block value,
            @BlockIndex int position,
            @SqlType(INTEGER) long desiredSampleSize)
    {
        checkArgument(desiredSampleSize > 0, "desired sample size must be > 0");
        if (initialProcessedCount <= 0) {
            // initial state block must be null or empty to prevent confusing situation where the
            // initial sample is not used
            checkArgument(initialState.isNull(initialStatePos) || initialState.getBlock(initialStatePos).getPositionCount() == 0, "initial state array must be null or empty when initial processed count is <= 0");
        }
        if (state.get() == null) {
            state.set(new ReservoirSample(type));
        }
        ReservoirSample sample = state.get();
        sample.tryInitialize((int) desiredSampleSize);

        Block initialStateBlock = null;
        if (initialProcessedCount > 0) {
            initialStateBlock = initialState.getBlock(initialStatePos);
        }
        sample.initializeInitialSample(initialStateBlock, initialProcessedCount);
        sample.add(value, position);
    }

    @CombineFunction
    public static void combine(
            @AggregationState ReservoirSampleState state,
            @AggregationState ReservoirSampleState otherState)
    {
        if (state.get() == null) {
            state.set(otherState.get());
            return;
        }
        state.get().mergeWith(otherState.get());
    }

    @OutputFunction("row(processed_count bigint, sample array(T))")
    public static void output(
            @TypeParameter("T") Type elementType,
            @AggregationState ReservoirSampleState state,
            BlockBuilder out)
    {
        ReservoirSample reservoirSample = state.get();
        final Block initialSampleBlock = Optional.ofNullable(reservoirSample.getInitialSample())
                .orElseGet(() -> RunLengthEncodedBlock.create(elementType, null, 0));
        long initialProcessedCount = reservoirSample.getInitialProcessedCount();
        // merge the final state with the initial state given
        checkArgument(!(initialProcessedCount != -1 &&
                        initialProcessedCount != initialSampleBlock.getPositionCount()) ||
                        reservoirSample.getMaxSampleSize() == initialSampleBlock.getPositionCount(),
                "when a positive initial_processed_count is provided the size of " +
                        "the initial sample must be equal to desired_sample_size parameter");
        ReservoirSample finalSample = new ReservoirSample(elementType, max(initialProcessedCount, 0), reservoirSample.getMaxSampleSize(), initialSampleBlock, null, 0);
        finalSample.merge(reservoirSample);

        long count = finalSample.getProcessedCount();
        BlockBuilder entryBuilder = out.beginBlockEntry();
        BigintType.BIGINT.writeLong(entryBuilder, count);
        BlockBuilder sampleBlock = finalSample.getSampleBlockBuilder();
        reservoirSample.getArrayType().appendTo(sampleBlock.build(), 0, entryBuilder);
        out.closeEntry();
    }
}
