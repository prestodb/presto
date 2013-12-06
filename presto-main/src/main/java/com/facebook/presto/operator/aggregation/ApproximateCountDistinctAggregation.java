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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.block.BlockBuilder.DEFAULT_MAX_BLOCK_SIZE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkState;

public class ApproximateCountDistinctAggregation
        extends SimpleAggregationFunction
{
    public static final ApproximateCountDistinctAggregation LONG_INSTANCE = new ApproximateCountDistinctAggregation(new LongHasher());
    public static final ApproximateCountDistinctAggregation DOUBLE_INSTANCE = new ApproximateCountDistinctAggregation(new DoubleHasher());
    public static final ApproximateCountDistinctAggregation VARBINARY_INSTANCE = new ApproximateCountDistinctAggregation(new SliceHasher());

    private static final HyperLogLog ESTIMATOR = new HyperLogLog(2048);
    // 1 byte for null flag. We use the null flag to propagate a "null" field as intermediate
    // and thereby avoid sending a full list of buckets when no value has been added (just an optimization)
    private static final int ENTRY_SIZE = SizeOf.SIZE_OF_BYTE + ESTIMATOR.getSizeInBytes();
    private static final int SLICE_SIZE = Math.max(ENTRY_SIZE, Ints.checkedCast((DEFAULT_MAX_BLOCK_SIZE.toBytes() / ENTRY_SIZE) * ENTRY_SIZE));
    private static final int ENTRIES_PER_SLICE = SLICE_SIZE / ENTRY_SIZE;

    private final CursorHasher hasher;

    public ApproximateCountDistinctAggregation(CursorHasher hasher)
    {
        super(SINGLE_LONG, SINGLE_VARBINARY, hasher.getType());
        this.hasher = hasher;
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        return new ApproximateCountDistinctGroupedAccumulator(hasher, valueChannel);
    }

    public static class ApproximateCountDistinctGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final CursorHasher hasher;

        private final List<Slice> slices = new ArrayList<>();

        public ApproximateCountDistinctGroupedAccumulator(CursorHasher hasher, int valueChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_VARBINARY);
            this.hasher = hasher;
        }

        @Override
        public long getEstimatedSize()
        {
            return slices.size() * SLICE_SIZE;
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                // skip null values
                if (!values.isNull(0)) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    // todo do all of this with shifts and masks
                    long globalOffset = groupId * ENTRY_SIZE;
                    int sliceIndex = Ints.checkedCast(globalOffset / SLICE_SIZE);
                    Slice slice = slices.get(sliceIndex);
                    int sliceOffset = Ints.checkedCast(globalOffset - (sliceIndex * SLICE_SIZE));

                    long hash = hasher.hash(values, 0);

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
                if (!intermediates.isNull(0)) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    // todo do all of this with shifts and masks
                    long globalOffset = groupId * ENTRY_SIZE;
                    int sliceIndex = Ints.checkedCast(globalOffset / SLICE_SIZE);
                    Slice slice = slices.get(sliceIndex);
                    int sliceOffset = Ints.checkedCast(globalOffset - (sliceIndex * SLICE_SIZE));


                    Slice input = intermediates.getSlice(0);

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
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new ApproximateCountDistinctAccumulator(hasher, valueChannel);
    }

    public static class ApproximateCountDistinctAccumulator
            extends SimpleAccumulator
    {
        private final CursorHasher hasher;

        private final Slice slice = Slices.allocate(ENTRY_SIZE);
        private boolean notNull;

        public ApproximateCountDistinctAccumulator(CursorHasher hasher, int valueChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_VARBINARY);

            this.hasher = hasher;
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull(0)) {
                    notNull = true;

                    long hash = hasher.hash(values, 0);
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
                if (!intermediates.isNull(0)) {
                    notNull = true;

                    Slice input = intermediates.getSlice(0);
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

    public double getStandardError()
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

    public interface CursorHasher
    {
        Type getType();

        long hash(BlockCursor cursor, int field);
    }

    public static class DoubleHasher
            implements CursorHasher
    {
        private static final HashFunction HASH = Hashing.murmur3_128();

        @Override
        public Type getType()
        {
            return DOUBLE;
        }

        @Override
        public long hash(BlockCursor cursor, int field)
        {
            double value = cursor.getDouble(field);
            return HASH.hashLong(Double.doubleToLongBits(value)).asLong();
        }
    }

    public static class LongHasher
            implements CursorHasher
    {
        private static final HashFunction HASH = Hashing.murmur3_128();

        @Override
        public Type getType()
        {
            return FIXED_INT_64;
        }

        @Override
        public long hash(BlockCursor cursor, int field)
        {
            long value = cursor.getLong(field);
            return HASH.hashLong(value).asLong();
        }
    }

    public static class SliceHasher
            implements CursorHasher
    {
        private static final HashFunction HASH = Hashing.murmur3_128();

        @Override
        public Type getType()
        {
            return VARIABLE_BINARY;
        }

        @Override
        public long hash(BlockCursor cursor, int field)
        {
            Slice value = cursor.getSlice(field);

            return HASH.hashBytes(value.getBytes()).asLong();
        }
    }
}
