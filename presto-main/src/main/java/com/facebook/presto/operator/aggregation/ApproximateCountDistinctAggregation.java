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
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airlift.slice.Slice;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkNotNull;

public class ApproximateCountDistinctAggregation
        implements FixedWidthAggregationFunction
{
    public static final ApproximateCountDistinctAggregation LONG_INSTANCE = new ApproximateCountDistinctAggregation(new LongHasher());
    public static final ApproximateCountDistinctAggregation DOUBLE_INSTANCE = new ApproximateCountDistinctAggregation(new DoubleHasher());
    public static final ApproximateCountDistinctAggregation VARBINARY_INSTANCE = new ApproximateCountDistinctAggregation(new SliceHasher());

    private static final HyperLogLog ESTIMATOR = new HyperLogLog(2048);

    private final CursorHasher hasher;

    public ApproximateCountDistinctAggregation(CursorHasher hasher)
    {
        checkNotNull(hasher, "hasher is null");

        this.hasher = hasher;
    }

    public double getStandardError()
    {
        return ESTIMATOR.getStandardError();
    }

    @Override
    public int getFixedSize()
    {
        // 1 byte for null flag. We use the null flag to propagate a "null" field as intermediate
        // and thereby avoid sending a full list of buckets when no value has been added (just an optimization)
        return 1 + ESTIMATOR.getSizeInBytes();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_LONG;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_VARBINARY;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // we assume all bytes are initialized to 0
    }

    @Override
    public void addInput(int positionCount, Block block, int field, Slice valueSlice, int valueOffset)
    {
        boolean hasValue = false;

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(field)) {
                hasValue = true;

                long hash = hasher.hash(cursor, field);

                ESTIMATOR.update(hash, valueSlice, valueOffset + 1); // first byte is for null flag
            }
        }

        if (hasValue) {
            setNotNull(valueSlice, valueOffset);
        }
    }

    @Override
    public void addInput(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(field)) {
            return;
        }

        long hash = hasher.hash(cursor, field);

        ESTIMATOR.update(hash, valueSlice, valueOffset + 1);
        setNotNull(valueSlice, valueOffset);
    }

    @Override
    public void addIntermediate(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(field)) {
            return;
        }

        Slice input = cursor.getSlice(field);

        ESTIMATOR.mergeInto(valueSlice, valueOffset + 1, input, 0);
        setNotNull(valueSlice, valueOffset);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (isNull(valueSlice, valueOffset)) {
            output.appendNull();
        }
        else {
            Slice intermediate = valueSlice.slice(valueOffset + 1, ESTIMATOR.getSizeInBytes());
            output.append(intermediate); // TODO: add BlockBuilder.appendSlice(slice, offset, length) to avoid creating intermediate slice
        }
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (isNull(valueSlice, valueOffset)) {
            output.append(0);
            return;
        }

        output.append(ESTIMATOR.estimate(valueSlice, valueOffset + 1));
    }

    private boolean isNull(Slice valueSlice, int offset)
    {
        // first byte in value region is null flag
        return valueSlice.getByte(offset) == 0;
    }

    private void setNotNull(Slice valueSlice, int offset)
    {
        valueSlice.setByte(offset, 1);
    }

    public interface CursorHasher
    {
        long hash(BlockCursor cursor, int field);
    }

    public static class DoubleHasher
            implements CursorHasher
    {
        private static final HashFunction HASH = Hashing.murmur3_128();

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
        public long hash(BlockCursor cursor, int field)
        {
            Slice value = cursor.getSlice(field);

            return HASH.hashBytes(value.getBytes()).asLong();
        }
    }
}
