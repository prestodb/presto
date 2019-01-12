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
package io.prestosql.operator.aggregation.histogram;

import io.prestosql.array.IntBigArray;
import io.prestosql.array.LongBigArray;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.type.TypeUtils;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.operator.aggregation.histogram.HashUtil.calculateMaxFill;
import static io.prestosql.operator.aggregation.histogram.HashUtil.nextBucketId;
import static io.prestosql.operator.aggregation.histogram.HashUtil.nextProbeLinear;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

/**
 * implementation that uses groupId in the hash key, so that we may store all groupId x value -> count in one giant map. The value, however,
 * is normalized into a single ValueBuilder. Further, we do not have per-group instances of objects. In order to construct a histogram for a group on
 * demand, a linked list for each group is stored that overlays all virtual nodes (a shared index, i, across parallel arrays).
 * <p>
 * <pre>
 * eg,
 * heads[100] -> 3
 *
 * means the first piece of data is (values[valuePositions[3]], counts[3])
 * If next[3] is 10, then we look at (values[valuePositions[10]], counts[10])
 *
 * and so on to construct the value, count pairs needed. An iterator-style function, readAllValues, exists and the caller may do whatever want with the
 * values.
 *
 * </pre>
 */

public class GroupedTypedHistogram
        implements TypedHistogram
{
    private static final float MAX_FILL_RATIO = 0.5f;

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedTypedHistogram.class).instanceSize();
    private static final int EMPTY_BUCKET = -1;
    private static final int NULL = -1;
    private final int bucketId;

    private final Type type;
    private final BlockBuilder values;
    private final BucketNodeFactory bucketNodeFactory;
    //** these parallel arrays represent a node in the hash table; index -> int, value -> long
    private final LongBigArray counts;
    // need to store the groupId for a node for when we are doing value comparisons in hash lookups
    private final LongBigArray groupIds;
    // array of nodePointers (index in counts, valuePositions)
    private final IntBigArray nextPointers;
    // since we store histogram values in a hash, two histograms may have the same position (each unique value should have only one position in the internal
    // BlockBuilder of values; not that we extract a subset of this when constructing per-group-id histograms)
    private final IntBigArray valuePositions;
    // bucketId -> valueHash (no group, no mask)
    private final LongBigArray valueAndGroupHashes;
    //** end per-node arrays

    // per groupId, we have a pointer to the first in the list of nodes for this group
    private final LongBigArray headPointers;

    private IntBigArray buckets;
    private int nextNodePointer;
    private int mask;
    private int bucketCount;
    private int maxFill;
    // at most one thread uses this object at one time, so this must be set to the group being operated on
    private int currentGroupId = -1;
    private long numberOfGroups = 1;
    //
    private final ValueStore valueStore;

    public GroupedTypedHistogram(Type type, int expectedCount)
    {
        checkArgument(expectedCount > 0, "expectedSize must be greater than zero");
        this.type = type;
        this.bucketId = expectedCount;
        this.bucketCount = computeBucketCount(expectedCount, MAX_FILL_RATIO);
        this.mask = bucketCount - 1;
        this.maxFill = calculateMaxFill(bucketCount, MAX_FILL_RATIO);
        this.values = type.createBlockBuilder(null, computeBucketCount(expectedCount, GroupedTypedHistogram.MAX_FILL_RATIO));
        // buckets and node-arrays (bucket "points" to a node, so 1:1 relationship)
        buckets = new IntBigArray(-1);
        buckets.ensureCapacity(bucketCount);
        counts = new LongBigArray();
        valuePositions = new IntBigArray();
        valueAndGroupHashes = new LongBigArray();
        nextPointers = new IntBigArray(NULL);
        groupIds = new LongBigArray(-1);
        // here, one bucket is one node in the hash structure (vs a bucket may be a chain of nodes in closed-hashing with linked list hashing)
        // ie, this is open-address hashing
        resizeNodeArrays(bucketCount);
        // end bucket/node based arrays
        // per-group arrays: size will be set by external call, same as groups since the number will be the same
        headPointers = new LongBigArray(NULL);
        // index into counts/valuePositions
        nextNodePointer = 0;
        bucketNodeFactory = this.new BucketNodeFactory();
        valueStore = new ValueStore(expectedCount, values);
    }

    /**
     * TODO: use RowBlock in the future
     *
     * @param block of the form [key1, count1, key2, count2, ...]
     */
    public GroupedTypedHistogram(long groupId, Block block, Type type, int bucketId)
    {
        this(type, bucketId);
        currentGroupId = (int) groupId;
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
        valueAndGroupHashes.ensureCapacity(actualSize);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE
                + counts.sizeOf()
                + groupIds.sizeOf()
                + nextPointers.sizeOf()
                + valuePositions.sizeOf()
                + valueAndGroupHashes.sizeOf()
                + buckets.sizeOf()
                + values.getRetainedSizeInBytes()
                + valueStore.getEstimatedSize()
                + headPointers.sizeOf();
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
                checkArgument(nodePointer != NULL, "should never see null here as we exclude in iterateGroupNodesCall");
                ValueNode valueNode = bucketNodeFactory.createValueNode(nodePointer);
                valueNode.writeNodeAsBlock(values, blockBuilder);
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
            checkArgument(nodePointer != NULL, "should never see null here as we exclude in iterateGroupNodesCall");
            ValueNode valueNode = bucketNodeFactory.createValueNode(nodePointer);
            reader.read(values, valueNode.getValuePosition(), valueNode.getCount());
        });
    }

    @Override
    public TypedHistogram setGroupId(long groupId)
    {
        // TODO: should we change all indices into buckets nodePointers to longs?
        this.currentGroupId = (int) groupId;
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
        return bucketId;
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

    private void resizeTableIfNecessary()
    {
        if (nextNodePointer >= maxFill) {
            rehash();
        }
    }

    private static int computeBucketCount(int expectedSize, float maxFillRatio)
    {
        return arraySize(expectedSize, maxFillRatio);
    }

    private void addAll(long groupId, TypedHistogram other)
    {
        other.readAllValues((block, position, count) -> add(groupId, block, position, count));
    }

    private void add(long groupId, Block block, int position, long count)
    {
        resizeTableIfNecessary();

        BucketDataNode bucketDataNode = bucketNodeFactory.createBucketDataNode(groupId, block, position);

        if (bucketDataNode.processEntry(groupId, block, position, count)) {
            nextNodePointer++;
        }
    }

    private boolean isCurrentGroupEmpty()
    {
        return headPointers.get(currentGroupId) == NULL;
    }

    /**
     * used to iterate over all non-null nodes in the data structure
     *
     * @param nodeReader - will be passed every non-null nodePointer
     */
    private void iterateGroupNodes(long groupdId, NodeReader nodeReader)
    {
        // while the index can be a long, the value is always an int
        int currentPointer = (int) headPointers.get(groupdId);
        checkArgument(currentPointer != NULL, "valid group must have non-null head pointer");

        while (currentPointer != NULL) {
            checkState(currentPointer < nextNodePointer, "error, corrupt pointer; max valid %s, found %s", nextNodePointer, currentPointer);
            nodeReader.read(currentPointer);
            currentPointer = nextPointers.get(currentPointer);
        }
    }

    private void rehash()
    {
        long newBucketCountLong = bucketCount * 2L;

        if (newBucketCountLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed " + Integer.MAX_VALUE + " entries (" + newBucketCountLong + ")");
        }

        int newBucketCount = computeBucketCount((int) newBucketCountLong, MAX_FILL_RATIO);
        int newMask = newBucketCount - 1;
        IntBigArray newBuckets = new IntBigArray(-1);
        newBuckets.ensureCapacity(newBucketCount);

        for (int i = 0; i < nextNodePointer; i++) {
            // find the old one
            int bucketId = getBucketIdForNode(i, newMask);
            int probeCount = 1;

            int originalBucket = bucketId;
            // find new one
            while (newBuckets.get(bucketId) != -1) {
                int probe = nextProbe(probeCount);
                bucketId = nextBucketId(originalBucket, newMask, probe);
                probeCount++;
            }

            // record the mapping
            newBuckets.set(bucketId, i);
        }
        buckets = newBuckets;
        bucketCount = newBucketCount;
        maxFill = calculateMaxFill(newBucketCount, MAX_FILL_RATIO);
        mask = newMask;

        resizeNodeArrays(newBucketCount);
    }

    private int nextProbe(int probeCount)
    {
        return nextProbeLinear(probeCount);
    }

    //parallel arrays with data for
    private void resizeNodeArrays(int newBucketCount)
    {
        // every per-bucket array needs to be updated
        counts.ensureCapacity(newBucketCount);
        valuePositions.ensureCapacity(newBucketCount);
        nextPointers.ensureCapacity(newBucketCount);
        valueAndGroupHashes.ensureCapacity(newBucketCount);
        groupIds.ensureCapacity(newBucketCount);
    }

    private long combineGroupAndValueHash(long groupIdHash, long valueHash)
    {
        return groupIdHash ^ valueHash;
    }

    private int getBucketIdForNode(int nodePointer, int mask)
    {
        long valueAndGroupHash = valueAndGroupHashes.get(nodePointer); // without mask
        int bucketId = (int) (valueAndGroupHash & mask);

        return bucketId;
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
            checkState(nodePointer > -1, "ValueNode must point to a non-empty node");
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

        /**
         * given an output outputBlockBuilder, writes one row (key -> count) of our histogram
         *
         * @param valuesBlock - values.build() is called externally
         */
        void writeNodeAsBlock(Block valuesBlock, BlockBuilder outputBlockBuilder)
        {
            type.appendTo(valuesBlock, getValuePosition(), outputBlockBuilder);
            BIGINT.writeLong(outputBlockBuilder, getCount());
        }
    }

    // short-lived class that wraps a position in int buckets[] to help handle treating
    // it as a hash table with Nodes that have values
    private class BucketDataNode
    {
        // index into parallel arrays that are fields of a "node" in our hash table (eg counts, valuePositions)
        private final int bucketId;
        private final ValueNode valueNode;
        private final long valueHash;
        private final long valueAndGroupHash;
        private final int nodePointerToUse;
        private final boolean isEmpty;

        /**
         * @param bucketId - index into the bucket array. Depending on createBucketNode(), this is either empty node that requires setup handled in
         * processEntry->addNewGroup()
         * *
         * or one with an existing count and simply needs the count updated
         * <p>
         * processEntry handles these cases
         * @param valueAndGroupHash
         */
        private BucketDataNode(int bucketId, ValueNode valueNode, long valueHash, long valueAndGroupHash, int nodePointerToUse, boolean isEmpty)
        {
            this.bucketId = bucketId;
            this.valueNode = valueNode;
            this.valueHash = valueHash;
            this.valueAndGroupHash = valueAndGroupHash;
            this.nodePointerToUse = nodePointerToUse;
            this.isEmpty = isEmpty;
        }

        private boolean isEmpty()
        {
            return isEmpty;
        }

        /**
         * true iff needs to update nextNodePointer
         */
        private boolean processEntry(long groupId, Block block, int position, long count)
        {
            if (isEmpty()) {
                addNewGroup(groupId, block, position, count);
                return true;
            }
            else {
                valueNode.add(count);
                return false;
            }
        }

        private void addNewGroup(long groupId, Block block, int position, long count)
        {
            checkState(isEmpty(), "bucket %s not empty, points to %s", bucketId, buckets.get(bucketId));

            // we've already computed the value hash for only the value only; ValueStore will save it for future use
            int nextValuePosition = valueStore.addAndGetPosition(type, block, position, valueHash);
            // set value pointer to hash map of values
            valuePositions.set(nodePointerToUse, nextValuePosition);
            // save hashes for future rehashing
            valueAndGroupHashes.set(nodePointerToUse, valueAndGroupHash);
            // set pointer to node for this bucket
            buckets.set(bucketId, nodePointerToUse);
            // save data for this node
            counts.set(nodePointerToUse, count);
            // used for doing value comparisons on hash collisions
            groupIds.set(nodePointerToUse, groupId);
            // we only ever store ints as values; we need long as an index
            int currentHead = (int) headPointers.get(groupId);
            // maintain linked list of nodes in this group (insert at head)
            headPointers.set(groupId, nodePointerToUse);
            nextPointers.set(nodePointerToUse, currentHead);
        }
    }

    private class BucketNodeFactory
    {
        /**
         * invariant: "points" to a virtual node of [key, count] for the histogram and includes any indirection calcs. Makes not guarantees if the node is empty or not
         * (use isEmpty())
         */
        private BucketDataNode createBucketDataNode(long groupId, Block block, int position)
        {
            long valueHash = murmurHash3(TypeUtils.hashPosition(type, block, position));
            long groupIdHash = murmurHash3(groupId);
            long valueAndGroupHash = combineGroupAndValueHash(groupIdHash, valueHash);
            int bucketId = (int) (valueAndGroupHash & mask);
            int nodePointer;
            int probeCount = 1;
            int originalBucketId = bucketId;
            // look for an empty slot or a slot containing this group x key
            while (true) {
                nodePointer = buckets.get(bucketId);

                if (nodePointer == EMPTY_BUCKET) {
                    return new BucketDataNode(bucketId, new ValueNode(nextNodePointer), valueHash, valueAndGroupHash, nextNodePointer, true);
                }
                else if (groupAndValueMatches(groupId, block, position, nodePointer, valuePositions.get(nodePointer))) {
                    // value match
                    return new BucketDataNode(bucketId, new ValueNode(nodePointer), valueHash, valueAndGroupHash, nodePointer, false);
                }
                else {
                    // keep looking
                    int probe = nextProbe(probeCount);
                    bucketId = nextBucketId(originalBucketId, mask, probe);
                    probeCount++;
                }
            }
        }

        private boolean groupAndValueMatches(long groupId, Block block, int position, int nodePointer, int valuePosition)
        {
            long existingGroupId = groupIds.get(nodePointer);

            return existingGroupId == groupId && type.equalTo(block, position, values, valuePosition);
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
