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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.primitives.Ints;
import io.airlift.slice.Murmur3;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.block.BlockBuilderStatus.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class ApproximateCountDistinctAggregation
        extends SimpleAggregationFunction
{
    private static final HyperLogLog ESTIMATOR = new HyperLogLog(2048);
    // 1 byte for null flag. We use the null flag to propagate a "null" field as intermediate
    // and thereby avoid sending a full list of buckets when no value has been added (just an optimization)
    private static final int ENTRY_SIZE = SizeOf.SIZE_OF_BYTE + ESTIMATOR.getSizeInBytes();
    private static final int SLICE_SIZE = Math.max(ENTRY_SIZE, Ints.checkedCast((DEFAULT_MAX_BLOCK_SIZE_IN_BYTES / ENTRY_SIZE) * ENTRY_SIZE));
    private static final int ENTRIES_PER_SLICE = SLICE_SIZE / ENTRY_SIZE;

    private final Type parameterType;

    public ApproximateCountDistinctAggregation(Type parameterType)
    {
        super(BIGINT, VARCHAR, parameterType);

        checkArgument(parameterType == BIGINT || parameterType == DOUBLE || parameterType == VARCHAR,
                "Expected parameter type to be BIGINT, DOUBLE, or VARCHAR, but was %s",
                parameterType);

        this.parameterType = parameterType;
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "approximate count distinct does not support approximate queries");
        return new ApproximateCountDistinctGroupedAccumulator(parameterType, valueChannel, maskChannel);
    }

    public static class ApproximateCountDistinctGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final Type parameterType;
        private final List<Slice> slices = new ArrayList<>();

        public ApproximateCountDistinctGroupedAccumulator(Type parameterType, int valueChannel, Optional<Integer> maskChannel)
        {
            super(valueChannel, BIGINT, VARCHAR, maskChannel, Optional.<Integer>absent());
            this.parameterType = parameterType;
        }

        @Override
        public long getEstimatedSize()
        {
            return slices.size() * SLICE_SIZE;
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());

                // skip null values
                if (!values.isNull() && (masks == null || masks.getBoolean())) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    // todo do all of this with shifts and masks
                    long globalOffset = groupId * ENTRY_SIZE;
                    int sliceIndex = Ints.checkedCast(globalOffset / SLICE_SIZE);
                    Slice slice = slices.get(sliceIndex);
                    int sliceOffset = Ints.checkedCast(globalOffset - (sliceIndex * SLICE_SIZE));

                    long hash = hash(values, parameterType);

                    ESTIMATOR.update(hash, slice, sliceOffset + 1);
                    setNotNull(slice, sliceOffset);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediates = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());

                // skip null values
                if (!intermediates.isNull()) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    // todo do all of this with shifts and masks
                    long globalOffset = groupId * ENTRY_SIZE;
                    int sliceIndex = Ints.checkedCast(globalOffset / SLICE_SIZE);
                    Slice slice = slices.get(sliceIndex);
                    int sliceOffset = Ints.checkedCast(globalOffset - (sliceIndex * SLICE_SIZE));

                    Slice input = intermediates.getSlice();

                    ESTIMATOR.mergeInto(slice, sliceOffset + 1, input, 0);
                    setNotNull(slice, sliceOffset);
                }
            }
            checkState(!intermediates.advanceNextPosition());
        }

        private void ensureCapacity(long groupCount)
        {
            long neededPages = (groupCount + ENTRIES_PER_SLICE) / ENTRIES_PER_SLICE;
            while (slices.size() < neededPages) {
                slices.add(Slices.allocate(SLICE_SIZE));
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            // todo do all of this with shifts and masks
            long globalOffset = groupId * ENTRY_SIZE;
            int sliceIndex = Ints.checkedCast(globalOffset / SLICE_SIZE);
            Slice valueSlice = slices.get(sliceIndex);
            int valueOffset = Ints.checkedCast(globalOffset - (sliceIndex * SLICE_SIZE));

            if (isNull(valueSlice, valueOffset)) {
                output.appendNull();
            }
            else {
                Slice intermediate = valueSlice.slice(valueOffset + 1, ESTIMATOR.getSizeInBytes());
                output.append(intermediate); // TODO: add BlockBuilder.appendSlice(slice, offset, length) to avoid creating intermediate slice
            }
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            // todo do all of this with shifts and masks
            long globalOffset = groupId * ENTRY_SIZE;
            int sliceIndex = Ints.checkedCast(globalOffset / SLICE_SIZE);
            Slice valueSlice = slices.get(sliceIndex);
            int valueOffset = Ints.checkedCast(globalOffset - (sliceIndex * SLICE_SIZE));

            if (isNull(valueSlice, valueOffset)) {
                output.append(0);
            }
            else {
                output.append(ESTIMATOR.estimate(valueSlice, valueOffset + 1));
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "approximate count distinct does not support approximate queries");
        return new ApproximateCountDistinctAccumulator(parameterType, valueChannel, maskChannel);
    }

    public static class ApproximateCountDistinctAccumulator
            extends SimpleAccumulator
    {
        private final Type parameterType;

        private final Slice slice = Slices.allocate(ENTRY_SIZE);
        private boolean notNull;

        public ApproximateCountDistinctAccumulator(Type parameterType, int valueChannel, Optional<Integer> maskChannel)
        {
            super(valueChannel, BIGINT, VARCHAR, maskChannel, Optional.<Integer>absent());

            this.parameterType = parameterType;
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                if (!values.isNull() && (masks == null || masks.getBoolean())) {
                    notNull = true;

                    long hash = hash(values, parameterType);
                    ESTIMATOR.update(hash, slice, 0);
                }
            }
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                if (!intermediates.isNull()) {
                    notNull = true;

                    Slice input = intermediates.getSlice();
                    ESTIMATOR.mergeInto(slice, 0, input, 0);
                }
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            if (notNull) {
                out.append(slice);
            }
            else {
                out.appendNull();
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (notNull) {
                out.append(ESTIMATOR.estimate(slice, 0));
            }
            else {
                out.append(0);
            }
        }
    }

    public static double getStandardError()
    {
        return ESTIMATOR.getStandardError();
    }

    private static boolean isNull(Slice valueSlice, int offset)
    {
        // first byte in value region is null flag
        return valueSlice.getByte(offset) == 0;
    }

    private static void setNotNull(Slice valueSlice, int offset)
    {
        valueSlice.setByte(offset, 1);
    }

    private static long hash(BlockCursor values, Type parameterType)
    {
        if (parameterType == BIGINT) {
            long value = values.getLong();
            return Murmur3.hash64(value);
        }
        else if (parameterType == DOUBLE) {
            double value = values.getDouble();
            return Murmur3.hash64(Double.doubleToLongBits(value));
        }
        else if (parameterType == VARCHAR) {
            return Murmur3.hash64(values.getSlice());
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT, DOUBLE, or VARCHAR");
        }
    }
}
