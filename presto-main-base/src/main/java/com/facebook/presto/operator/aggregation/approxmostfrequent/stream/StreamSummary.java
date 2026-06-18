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
package com.facebook.presto.operator.aggregation.approxmostfrequent.stream;

import com.facebook.presto.common.array.IntBigArray;
import com.facebook.presto.common.array.LongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.type.TypeUtils;
import com.google.common.annotations.VisibleForTesting;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.google.common.base.Preconditions.checkArgument;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;

public class StreamSummary
        implements PriorityQueueDataChangeListener
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StreamSummary.class).instanceSize();
    private static final int COMPACT_THRESHOLD_BYTES = 32768;
    private static final float FILL_RATIO = 0.75f;
    private static final int COMPACT_THRESHOLD_RATIO = 3;
    private static final int EMPTY = -1;
    private static final int DELETE_MARKER = -2;
    private final Type type;
    private final int heapCapacity;
    private final int maxBuckets;
    private int maxFill;
    private int mask;
    private int generation;
    private LongBigArray blockPositionToCount;
    private IntBigArray hashToBlockPosition;
    private int hashCapacity;
    private BlockBuilder heapBlockBuilder;
    private final IndexedPriorityQueue minHeap;
    private IntBigArray blockToHeapIndex;

    public StreamSummary(
            Type type,
            int maxBuckets,
            int heapCapacity)
    {
        this.type = type;
        this.maxBuckets = maxBuckets;
        this.heapCapacity = heapCapacity;
        this.blockPositionToCount = new LongBigArray();
        this.blockToHeapIndex = new IntBigArray();
        this.hashToBlockPosition = new IntBigArray(EMPTY);
        this.hashCapacity = arraySize(heapCapacity, FILL_RATIO);
        this.hashToBlockPosition.ensureCapacity(hashCapacity);
        this.heapBlockBuilder = type.createBlockBuilder(null, heapCapacity);
        this.minHeap = new IndexedPriorityQueue(heapCapacity, this::compare, this);
        this.mask = hashCapacity - 1;
        this.maxFill = calculateMaxFill(hashCapacity);
        this.blockPositionToCount.ensureCapacity(hashCapacity);
        this.blockToHeapIndex.ensureCapacity(hashCapacity);
    }

    public void add(Block block, int blockPosition, long incrementCount)
    {
        int hashPosition = getBucketId(TypeUtils.hashPosition(type, block, blockPosition), mask);
        // look for empty slot or slot containing this key
        while (true) {
            int bucketPosition = hashToBlockPosition.get(hashPosition);
            if (bucketPosition == EMPTY) {
                break;
            }
            if (bucketPosition != DELETE_MARKER && type.equalTo(block, blockPosition, heapBlockBuilder, bucketPosition)) {
                blockPositionToCount.add(bucketPosition, incrementCount);
                int heapIndex = blockToHeapIndex.get(bucketPosition);
                minHeap.get(heapIndex).setGeneration(generation++);
                minHeap.percolateDown(heapIndex);
                return;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
        addNewGroup(block, blockPosition, hashPosition, incrementCount);
    }

    private void addNewGroup(Block block, int blockPosition, int hashPosition, long incrementCount)
    {
        int newElementBlockPosition = heapBlockBuilder.getPositionCount();
        if (minHeap.isFull()) {
            //replace min
            StreamDataEntity min = minHeap.getMin();
            int removedBlock = getBlockPosition(min);
            long minCount = blockPositionToCount.get(removedBlock);
            handleDelete(removedBlock, min.getHashPosition());

            hashToBlockPosition.set(hashPosition, newElementBlockPosition);
            blockPositionToCount.set(newElementBlockPosition, minCount + incrementCount);
            minHeap.replaceMin(new StreamDataEntity(hashPosition, generation++));
        }
        else {
            hashToBlockPosition.set(hashPosition, newElementBlockPosition);
            blockPositionToCount.set(newElementBlockPosition, incrementCount);
            minHeap.add(new StreamDataEntity(hashPosition, generation++));
        }
        type.appendTo(block, blockPosition, heapBlockBuilder);
        compactAndRehashIfNeeded();
    }

    private void handleDelete(int removedBlock, int removedHashPosition)
    {
        blockPositionToCount.set(removedBlock, 0);
        blockToHeapIndex.set(removedBlock, EMPTY);
        hashToBlockPosition.set(removedHashPosition, DELETE_MARKER);
    }

    private void compactAndRehashIfNeeded()
    {
        if (shouldCompact(heapBlockBuilder.getSizeInBytes(), heapBlockBuilder.getPositionCount())) {
            compact();
        }
        else {
            if (heapBlockBuilder.getPositionCount() >= maxFill) {
                rehash();
            }
        }
    }

    protected boolean shouldCompact(long sizeInBytes, int numberOfPositionInBlock)
    {
        return sizeInBytes >= COMPACT_THRESHOLD_BYTES && numberOfPositionInBlock / getHeapSize() >= COMPACT_THRESHOLD_RATIO;
    }

    @VisibleForTesting
    public int getHeapSize()
    {
        return minHeap.getSize();
    }

    private synchronized void compact()
    {
        BlockBuilder newHeapBlockBuilder = type.createBlockBuilder(null, heapBlockBuilder.getPositionCount());
        //since block positions are changed, we need to update all data structures which are using block position as reference
        LongBigArray newBlockPositionToCount = new LongBigArray();
        hashCapacity = arraySize(heapCapacity, FILL_RATIO);
        maxFill = calculateMaxFill(hashCapacity);
        newBlockPositionToCount.ensureCapacity(hashCapacity);
        IntBigArray newBlockToHeapIndex = new IntBigArray();
        newBlockToHeapIndex.ensureCapacity(hashCapacity);

        for (int heapPosition = 0; heapPosition < getHeapSize(); heapPosition++) {
            int newBlockPos = newHeapBlockBuilder.getPositionCount();
            StreamDataEntity heapEntry = minHeap.get(heapPosition);
            int oldBlockPosition = getBlockPosition(heapEntry);
            type.appendTo(heapBlockBuilder, oldBlockPosition, newHeapBlockBuilder);
            newBlockPositionToCount.set(newBlockPos, blockPositionToCount.get(oldBlockPosition));
            newBlockToHeapIndex.set(newBlockPos, heapPosition);
            hashToBlockPosition.set(heapEntry.getHashPosition(), newBlockPos);
        }
        blockPositionToCount = newBlockPositionToCount;
        heapBlockBuilder = newHeapBlockBuilder;
        blockToHeapIndex = newBlockToHeapIndex;
        rehash();
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;
        int newMask = newCapacity - 1;
        IntBigArray newHashToBlockPosition = new IntBigArray(EMPTY);
        newHashToBlockPosition.ensureCapacity(newCapacity);

        for (int heapPosition = 0; heapPosition < getHeapSize(); heapPosition++) {
            StreamDataEntity heapEntry = minHeap.get(heapPosition);
            int blockPosition = getBlockPosition(heapEntry);
            // find an empty slot for the address
            int hashPosition = getBucketId(TypeUtils.hashPosition(type, heapBlockBuilder, blockPosition), newMask);

            while (newHashToBlockPosition.get(hashPosition) != EMPTY) {
                hashPosition = (hashPosition + 1) & newMask;
            }
            // record the mapping
            newHashToBlockPosition.set(hashPosition, blockPosition);
            heapEntry.setHashPosition(hashPosition);
        }
        hashCapacity = newCapacity;
        mask = newMask;
        maxFill = calculateMaxFill(newCapacity);
        this.hashToBlockPosition = newHashToBlockPosition;
        this.blockPositionToCount.ensureCapacity(maxFill);
        this.blockToHeapIndex.ensureCapacity(maxFill);
    }

    private int compare(StreamDataEntity heapValue1, StreamDataEntity heapValue2)
    {
        int compare = Long.compare(
                getCount(heapValue1),
                getCount(heapValue2));
        if (compare == 0) {
            //When the counts are same, we want to consider the previously generated value as minimum
            // to prefer it over newly generated value with same count when remove min is called
            compare = Long.compare(heapValue1.getGeneration(), heapValue2.getGeneration());
        }
        return compare;
    }

    private long getCount(StreamDataEntity heapEntry)
    {
        return blockPositionToCount.get(getBlockPosition(heapEntry));
    }

    private int getBlockPosition(StreamDataEntity heapEntry)
    {
        return hashToBlockPosition.get(heapEntry.getHashPosition());
    }

    private static int getBucketId(long rawHash, int mask)
    {
        return ((int) murmurHash3(rawHash)) & mask;
    }

    public void topK(BlockBuilder out)
    {
        //get top k heap entries
        List<StreamDataEntity> sortedHeapEntries = getTopHeapEntries();
        //write data and count to output
        BlockBuilder valueBuilder = out.beginBlockEntry();
        for (StreamDataEntity heapEntry : sortedHeapEntries) {
            type.appendTo(heapBlockBuilder, getBlockPosition(heapEntry), valueBuilder);
            BIGINT.writeLong(valueBuilder, getCount(heapEntry));
        }
        out.closeEntry();
    }

    private List<StreamDataEntity> getTopHeapEntries()
    {
        return minHeap.topK(maxBuckets, (heapEntry1, heapEntry2) -> {
            int compare = Long.compare(
                    getCount(heapEntry2),
                    getCount(heapEntry1));
            if (compare == 0) {
                return Integer.compare(heapEntry1.getGeneration(), heapEntry2.getGeneration());
            }
            return compare;
        });
    }

    public void merge(StreamSummary otherStreamSummary)
    {
        otherStreamSummary.readAllValues(this::add);
    }

    public void readAllValues(StreamSummaryReader reader)
    {
        List<StreamDataEntity> heapEntries = getTopHeapEntries();
        for (StreamDataEntity heapEntry : heapEntries) {
            reader.read(heapBlockBuilder, getBlockPosition(heapEntry), getCount(heapEntry));
        }
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder blockBuilder = out.beginBlockEntry();
        if (getHeapSize() > 0) {
            BIGINT.writeLong(blockBuilder, maxBuckets);
            BIGINT.writeLong(blockBuilder, heapCapacity);
            List<StreamDataEntity> sortedHeap = getTopHeapEntries();
            BlockBuilder keyItems = blockBuilder.beginBlockEntry();
            for (StreamDataEntity heapEntry : sortedHeap) {
                type.appendTo(heapBlockBuilder, getBlockPosition(heapEntry), keyItems);
            }
            blockBuilder.closeEntry();
            BlockBuilder valueItems = blockBuilder.beginBlockEntry();
            for (StreamDataEntity heapEntry : sortedHeap) {
                BIGINT.writeLong(valueItems, getCount(heapEntry));
            }
            blockBuilder.closeEntry();
        }
        out.closeEntry();
    }

    public static StreamSummary deserialize(Type type, Block block)
    {
        int currentPosition = 0;
        int maxBuckets = toIntExact(BIGINT.getLong(block, currentPosition++));
        int heapCapacity = toIntExact(BIGINT.getLong(block, currentPosition++));

        StreamSummary streamSummary = new StreamSummary(type, maxBuckets, heapCapacity);
        Block keysBlock = new ArrayType(type).getObject(block, currentPosition++);
        Block valuesBlock = new ArrayType(BIGINT).getObject(block, currentPosition);

        for (int position = 0; position < keysBlock.getPositionCount(); position++) {
            streamSummary.add(keysBlock, position, valuesBlock.getLong(position));
        }
        return streamSummary;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + heapBlockBuilder.getRetainedSizeInBytes() + minHeap.estimatedInMemorySize() + blockPositionToCount.sizeOf() +
                hashToBlockPosition.sizeOf();
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

    @Override
    public void indexChanged(StreamDataEntity blockReferenceEntity, int newIndex)
    {
        blockToHeapIndex.set(getBlockPosition(blockReferenceEntity), newIndex);
    }
}
