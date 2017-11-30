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

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.operator.aggregation.state.GroupedHistogramState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalLimit;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

public class GroupedTypedHistogramSharedValues
        implements TypedHistogram
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedTypedHistogramSharedValues.class).instanceSize();
    private static final int EMPTY_BUCKET = -1;

    private static final float FILL_RATIO = 0.9f;
    private static final long FOUR_MEGABYTES = new DataSize(4, MEGABYTE).toBytes();

    private final int expectedEntriesCount;
    private int bucketCount;
    private int maxFill;
    private int mask;
    private int nextNodePointer;

    private final Type type;
    private final BlockBuilder values;

    private IntBigArray buckets;
    private final LongBigArray counts;
    private final IntBigArray valuePositions;
    private final DataNodeFactory dataNodeFactory;

    public GroupedTypedHistogramSharedValues(Type type, int expectedEntriesCount, BlockBuilder values)
    {
        this.type = type;
        this.expectedEntriesCount = expectedEntriesCount;
        this.bucketCount = GroupedHistogramState.computeBucketCount(expectedEntriesCount, FILL_RATIO);
        this.values = values;

        checkArgument(expectedEntriesCount > 0, "expectedSize must be greater than zero");

        maxFill = calculateMaxFill(bucketCount);
        mask = bucketCount - 1;
        buckets = new IntBigArray(-1);
        buckets.ensureCapacity(bucketCount);
        counts = new LongBigArray();
        counts.ensureCapacity(bucketCount);
        valuePositions = new IntBigArray();
        valuePositions.ensureCapacity(bucketCount);
        // index into counts/valuePositions
        nextNodePointer = 0;
        dataNodeFactory = this.new DataNodeFactory();
    }

    @Deprecated
    @VisibleForTesting
    GroupedTypedHistogramSharedValues(Type type, int expectedEntriesCount)
    {
        this(type, expectedEntriesCount, type.createBlockBuilder(null, GroupedHistogramState.computeBucketCount(expectedEntriesCount, FILL_RATIO)));
    }

    /**
     * @param block of the form [key1, count1, key2, count2, ...]
     * @param type
     * @param expectedEntriesCount
     */
    public GroupedTypedHistogramSharedValues(Block block, Type type, int expectedEntriesCount, BlockBuilder valuesBlockBlockbuilder)
    {
        this(type, expectedEntriesCount, valuesBlockBlockbuilder);
        requireNonNull(block, "block is null");
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(i, block, BIGINT.getLong(block, i + 1));
        }
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + values.getRetainedSizeInBytes() + counts.sizeOf() + buckets.sizeOf() + valuePositions.sizeOf();
    }

    @Override
    public void serialize(BlockBuilder out)
    {
        if (nextNodePointer == 0) {
            out.appendNull();
        }
        else {
            // do we need to build this?
            Block valuesBlock = values;
            BlockBuilder blockBuilder = out.beginBlockEntry();

            for (int i = 0; i < nextNodePointer; i++) {
                ValueNode valueNode = dataNodeFactory.createValueNode(i);
                valueNode.writeNodeAsRow(valuesBlock, blockBuilder);
            }

            out.closeEntry();
        }
    }

    @Override
    public void addAll(TypedHistogram other)
    {
        other.readAllValues((block, position, count) -> add(position, block, count));
    }

    @Override
    public void readAllValues(HistogramValueReader reader)
    {
        for (int i = 0; i < nextNodePointer; i++) {
            ValueNode valueNode = dataNodeFactory.createValueNode(i);
            reader.read(values, valueNode.getValuePosition(), valueNode.getCount());
        }
    }

    @Override
    public void add(int position, Block block, long count)
    {
        BucketDataNode bucketDataNode = dataNodeFactory.createBucketDataNode(block, position);

        if (bucketDataNode.isEmpty()) {
            bucketDataNode.addNewGroup(block, position, count);
        }
        else {
            bucketDataNode.add(count);
        }
    }


    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public int getExpectedSize()
    {
        return expectedEntriesCount;
    }

    @Override
    public boolean isEmpty()
    {
        return nextNodePointer == 0;
    }

    private void rehash()
    {
        long newCapacityLong = bucketCount * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;

        int newMask = newCapacity - 1;
        IntBigArray newBuckets = new IntBigArray(-1);
        newBuckets.ensureCapacity(newCapacity);

        for (int i = 0; i < valuePositions.sizeOf(); i++) {
            // find an empty slot for the address
            int bucketId = getHashPosition(TypeUtils.hashPosition(type, values, i), newMask);
            while (newBuckets.get(bucketId) != -1) {
                bucketId = (bucketId + 1) & newMask;
            }

            // record the mapping
            newBuckets.set(bucketId, i);
        }

        bucketCount = newCapacity;
        mask = newMask;
        maxFill = calculateMaxFill(newCapacity);
        buckets = newBuckets;

        counts.ensureCapacity(maxFill);
        valuePositions.ensureCapacity(maxFill);
    }

    private static int getHashPosition(long rawHash, int mask)
    {
        return ((int) murmurHash3(rawHash)) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private int nextBucketId(int bucketId)
    {
        // increment position and mask to handle wrap around
        return (bucketId + 1) & mask;
    }

    //short-lived abstraction that is basically a position into parallel arrays that we can treat as one data structure
    private class ValueNode
    {
        // index into parallel arrays that are fields of a "node" in our hash table (eg counts, valuePositions)
        private final int nodePointer;

        /**
         * @param nodePointer - index/pointer into parallel arrays of data structs
         */
        private ValueNode(int nodePointer)
        {
            this.nodePointer = nodePointer;
        }

        private long getCount()
        {
            return counts.get(nodePointer);
        }

        int getValuePosition()
        {
            return valuePositions.get(nodePointer);
        }

        void add(long count)
        {
            counts.add(nodePointer, count);
        }

        /**
         * given an output outputBlockBuilder, writes one row (key -> count) of our histogram
         *
         * @param valuesBlock - values.build() is called externally
         * @param outputBlockBuilder
         */
        private void writeNodeAsRow(Block valuesBlock, BlockBuilder outputBlockBuilder)
        {
            type.appendTo(valuesBlock, getValuePosition(), outputBlockBuilder);
            BIGINT.writeLong(outputBlockBuilder, getCount());
        }
    }

    // short-lived class that wraps a position in int buckets[] to help handle treating
    // it as a hash table with Nodes that have values
    private class BucketDataNode
            extends ValueNode
    {
        // index into parallel arrays that are fields of a "node" in our hash table (eg counts, valuePositions)
        private final int bucketId;

        /**
         * @param bucketId - index into the bucket array. It is writable. If ! EMPTY_BUCKET, it's value points to valid counts and pointers in
         * additional data structures
         * @param nodePointer - index/pointer into parallel arrays of data structs
         */
        private BucketDataNode(int bucketId, int nodePointer)
        {
            super(nodePointer);
            this.bucketId = bucketId;
        }

        private boolean isEmpty()
        {
            return buckets.get(bucketId) == EMPTY_BUCKET;
        }

        private void addNewGroup(Block block, int position, long count)
        {
            Preconditions.checkState(isEmpty(), "bucket %s not empty, points to %s", bucketId, buckets.get(bucketId));

            int nextValuePosition = values.getPositionCount();
            buckets.set(bucketId, nextNodePointer);
            counts.set(nextNodePointer, count);
            valuePositions.set(nextNodePointer, nextValuePosition);
            type.appendTo(block, position, values);
            // increase capacity, if necessary
            if (nextNodePointer++ >= maxFill) {
                rehash();
            }
            // TODO: we'll want some way to combine the unique + shared data
            if (getEstimatedSize() > FOUR_MEGABYTES) {
                throw exceededLocalLimit(new DataSize(4, MEGABYTE));
            }
        }
    }

    private class DataNodeFactory
    {
        /**
         * invariant: "points" to a virtual node of [key, count] for the histogram and includes any indirection calculations
         *
         * @param block
         * @param position
         */
        private BucketDataNode createBucketDataNode(Block block, int position)
        {
            int bucketId = getHashPosition(TypeUtils.hashPosition(type, block, position), mask);
            int nodePointer;

            // look for an empty slot or a slot containing this key
            while (true) {
                nodePointer = buckets.get(bucketId);
                // empty
                if (nodePointer == EMPTY_BUCKET) {
                    break;
                }
                int valuePosition = valuePositions.get(nodePointer);
                // value match
                if (type.equalTo(block, position, values, valuePosition)) {
                    break;
                }

                bucketId = nextBucketId(bucketId);
            }

            return new BucketDataNode(bucketId, nodePointer);
        }

        private ValueNode createValueNode(int nodePointer)
        {
            return new ValueNode(nodePointer);
        }
    }
}
