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
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalLimit;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

public class GroupedTypedHistogram
        implements TypedHistogram
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedTypedHistogram.class).instanceSize();
    private static final int EMPTY_BUCKET = -1;
    private static final int NULL = -1;
    private static final float FILL_RATIO = 0.9f;

    private static final long FOUR_MEGABYTES = new DataSize(4, MEGABYTE).toBytes();
    private final int expectedEntriesCount;

    private final Type type;
    private final BlockBuilder values;
    private final DataNodeFactory dataNodeFactory;
    // these parallel arrays represent a node in the hash table
    private final LongBigArray counts;
    private final LongBigArray groupIds;
    private final IntBigArray valuePositions;
    // array of nodePointers (index in counts, valuePositions)
    private final IntBigArray nextPointers;
    // per groupId, we have a pointer to the first in the list of nodes for this group
    private final LongBigArray headPointers;

    private IntBigArray buckets;
    private int nextNodePointer;
    private int mask;
    private int bucketCount;
    private int maxFill;
    // at most one thread uses this object at one time, so this must be set to the group being operated on
    private long currentGroupId = -1;
    private long numberOfGroups = 1;

    public GroupedTypedHistogram(Type type, int expectedEntriesCount, BlockBuilder values)
    {
        this.type = type;
        this.expectedEntriesCount = expectedEntriesCount;
        this.bucketCount = computeBucketCount(expectedEntriesCount);
        this.mask = bucketCount - 1;
        this.maxFill = calculateMaxFill(bucketCount);
        this.values = values;

        checkArgument(expectedEntriesCount > 0, "expectedSize must be greater than zero");

        buckets = new IntBigArray(-1);
        buckets.ensureCapacity(bucketCount);
        counts = new LongBigArray();
        counts.ensureCapacity(maxFill);
        groupIds = new LongBigArray(-1);
        valuePositions = new IntBigArray();
        valuePositions.ensureCapacity(maxFill);
        nextPointers = new IntBigArray(NULL);
        nextPointers.ensureCapacity(maxFill);
        // size will be set by external call, other
        headPointers = new LongBigArray(NULL);

        // index into counts/valuePositions
        nextNodePointer = 0;
        dataNodeFactory = this.new DataNodeFactory();
    }

    /**
     * @param groupId
     * @param block of the form [key1, count1, key2, count2, ...]
     * @param type
     * @param expectedEntriesCount
     */
    public GroupedTypedHistogram(long groupId, Block block, Type type, int expectedEntriesCount, BlockBuilder valuesBlockBuilde)
    {
        this(type, expectedEntriesCount, valuesBlockBuilde);
        currentGroupId = groupId;
        requireNonNull(block, "block is null");
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(groupId, block, i, BIGINT.getLong(block, i + 1));
        }
    }

    @Override
    public void ensureCapacity(long size)
    {
        long actualSize = Math.max(numberOfGroups, size);
        this.numberOfGroups = actualSize;
        headPointers.ensureCapacity(actualSize);
        groupIds.ensureCapacity(actualSize);
    }

    @Override
    public long getEstimatedSize()
    {
        // TODO: need to include new fields
        return INSTANCE_SIZE + buckets.sizeOf() + values.getRetainedSizeInBytes() + counts.sizeOf() + valuePositions.sizeOf() + nextPointers.sizeOf() + headPointers.sizeOf();
    }

    @Override
    public void serialize(BlockBuilder out)
    {
        if (isCurrentGroupEmpty()) {
            out.appendNull();
        }
        else {
            BlockBuilder blockBuilder = out.beginBlockEntry();
            iterateGroupNodes(currentGroupId, nodePointer -> {
                Preconditions.checkArgument(nodePointer != NULL, "should never see null here as we exclude above");
                // we only ever store ints
                ValueNode valueNode = dataNodeFactory.createValueNode(nodePointer);
                valueNode.writeNodeAsRow(values, blockBuilder);
            });
            out.closeEntry();
        }
    }

    @Override
    public void addAll(TypedHistogram other)
    {
        addAll(currentGroupId, other);
    }

    @Override
    public void readAllValues(HistogramValueReader reader)
    {
        iterateGroupNodes(currentGroupId, nodePointer -> {
            if (nodePointer != NULL) {
                ValueNode valueNode = dataNodeFactory.createValueNode(nodePointer);
                reader.read(values, valueNode.getValuePosition(), valueNode.getCount());
            }
        });
    }

    @Override
    public TypedHistogram setGroupId(long groupId)
    {
        this.currentGroupId = groupId;
        return this;
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
        return isCurrentGroupEmpty();
    }

    @Override
    public void add(int position, Block block, long count)
    {
        checkState(currentGroupId != -1, "setGroupId() not called yet");
        add(currentGroupId, block, position, count);
    }

    private static int computeBucketCount(int expectedSize)
    {
        return GroupedHistogramState.computeBucketCount(expectedSize, FILL_RATIO);
    }

    private void addAll(long groupId, TypedHistogram other)
    {
        other.readAllValues((block, position, count) -> add(groupId, block, position, count));
    }

    private void add(long groupId, Block block, int position, long count)
    {
        BucketDataNode bucketDataNode = dataNodeFactory.createBucketDataNode(groupId, block, position);

        if (bucketDataNode.isEmpty()) {
            bucketDataNode.addNewGroup(groupId, block, position, count);
        }
        else {
            bucketDataNode.add(count);
        }
    }

    private boolean isCurrentGroupEmpty()
    {
        return headPointers.get(currentGroupId) == NULL;
    }

    private void iterateGroupNodes(long groupdId, NodeReader nodeReader)
    {
        int currentPointer = (int) headPointers.get(groupdId);
        if (currentPointer == NULL) {
            nodeReader.read(NULL);
        }
        else {
            while (currentPointer != NULL) {
                checkState(currentPointer < nextNodePointer, "error, corrupt pointer; max valid %s, found %s", nextNodePointer, currentPointer);
                nodeReader.read(currentPointer);
                currentPointer = nextPointers.get(currentPointer);
            }
        }
    }

    private void rehash(long groupId)
    {
        long newBucketCountLong = bucketCount * 2L;
        if (newBucketCountLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newBucketCount = (int) newBucketCountLong;

        int newMask = newBucketCount - 1;
        IntBigArray newBuckets = new IntBigArray(-1);
        newBuckets.ensureCapacity(newBucketCount);
        for (int i = 0; i < valuePositions.sizeOf(); i++) {
            // find an empty slot for the address
            int bucketId = getHashPosition(groupId, TypeUtils.hashPosition(type, values, i), newMask);
            while (newBuckets.get(bucketId) != -1) {
                bucketId = (bucketId + 1) & newMask;
            }

            // record the mapping
            newBuckets.set(bucketId, i);
        }

        buckets = newBuckets;
        maxFill = calculateMaxFill(newBucketCount);
        // every per-bucket array needs to be updated
        counts.ensureCapacity(newBucketCount);
        valuePositions.ensureCapacity(newBucketCount);
        nextPointers.ensureCapacity(newBucketCount);
    }

    private int getHashPosition(long groupId, long valueRawHash, int mask)
    {
        return ((int) murmurHash3((murmurHash3(valueRawHash) ^ murmurHash3(groupId)))) & mask;
    }

    private static int calculateMaxFill(int bucketCount)
    {
        checkArgument(bucketCount > 0, "bucketCount must be greater than 0");
        int maxFill = (int) Math.ceil(bucketCount * FILL_RATIO);
        if (maxFill == bucketCount) {
            maxFill--;
        }
        checkArgument(bucketCount > maxFill, "bucketCount must be larger than maxFill");
        return maxFill;
    }

    private int nextBucketId(int bucketId, int mask)
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
        ValueNode(int nodePointer)
        {
            this.nodePointer = nodePointer;
        }

        long getCount()
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

        int getNextPointer()
        {
            return (int) counts.get(nodePointer);
        }

        /**
         * given an output outputBlockBuilder, writes one row (key -> count) of our histogram
         *
         * @param valuesBlock - values.build() is called externally
         * @param outputBlockBuilder
         */
        void writeNodeAsRow(Block valuesBlock, BlockBuilder outputBlockBuilder)
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
         * @param currentBuckets
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

        private void addNewGroup(long groupId, Block block, int position, long count)
        {
            checkState(isEmpty(), "bucket %s not empty, points to %s", bucketId, buckets.get(bucketId));

            int nextValuePosition = values.getPositionCount();
            buckets.set(bucketId, nextNodePointer);
            counts.set(nextNodePointer, count);
            valuePositions.set(nextNodePointer, nextValuePosition);
            groupIds.set(nextNodePointer, groupId);
            // we only ever store ints as values; we need long as an index
            int currentHead = (int) headPointers.get(groupId);
            headPointers.set(groupId, nextNodePointer);
            nextPointers.set(nextNodePointer, currentHead);
            type.appendTo(block, position, values);
            // increase capacity, if necessary
            if (nextNodePointer++ >= mask) {
                rehash(groupId);
            }

            if (getEstimatedSize() > numberOfGroups * FOUR_MEGABYTES) {
                throw exceededLocalLimit(getMaxDataSize());
            }
        }
    }

    private DataSize getMaxDataSize()
    {
        return new DataSize(numberOfGroups * 4, MEGABYTE);
    }

    private class DataNodeFactory
    {
        /**
         * invariant: "points" to a virtual node of [key, count] for the histogram and includes any indirection calculations
         *
         * @param currentBuckets
         * @param i
         * @param groupId
         * @param block
         * @param position
         */
        private BucketDataNode createBucketDataNode(long groupId, Block block, int position)
        {
            int bucketId = getHashPosition(groupId, TypeUtils.hashPosition(type, block, position), mask);
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
                if (groupAndValueMatches(groupId, block, position, nodePointer, valuePosition)) {
                    break;
                }

                bucketId = nextBucketId(bucketId, mask);
            }

            return new BucketDataNode(bucketId, nodePointer);
        }

        private boolean groupAndValueMatches(long groupId, Block block, int position, int nodePointer, int valuePosition)
        {
            return groupIds.get(nodePointer) == groupId && type.equalTo(block, position, values, valuePosition);
        }

        private ValueNode createValueNode(int nodePointer)
        {
            return new ValueNode(nodePointer);
        }
    }

    private interface NodeReader
    {
        void read(int nodePointer);
    }
}
