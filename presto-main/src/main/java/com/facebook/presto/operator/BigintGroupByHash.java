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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.array.IntBigArray;
import com.facebook.presto.common.array.LongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.type.BigintOperators;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
import static com.facebook.presto.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BigintGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintGroupByHash.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> TYPES_WITH_RAW_HASH = ImmutableList.of(BIGINT, BIGINT);

    private final int hashChannel;
    private final boolean outputRawHash;

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table from values to groupIds
    private LongBigArray values;
    private IntBigArray groupIds;

    // groupId for the null value
    private int nullGroupId = -1;

    // reverse index from the groupId back to the value
    private final LongBigArray valuesByGroupId;

    private int nextGroupId;
    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    public BigintGroupByHash(int hashChannel, boolean outputRawHash, int expectedSize, UpdateMemory updateMemory)
    {
        checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.hashChannel = hashChannel;
        this.outputRawHash = outputRawHash;

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        values = new LongBigArray();
        values.ensureCapacity(hashCapacity);
        groupIds = new IntBigArray(-1);
        groupIds.ensureCapacity(hashCapacity);

        valuesByGroupId = new LongBigArray();
        valuesByGroupId.ensureCapacity(hashCapacity);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                groupIds.sizeOf() +
                values.sizeOf() +
                valuesByGroupId.sizeOf() +
                preallocatedMemoryInBytes;
    }

    @Override
    public long getHashCollisions()
    {
        return hashCollisions;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions + estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);
    }

    @Override
    public List<Type> getTypes()
    {
        return outputRawHash ? TYPES_WITH_RAW_HASH : TYPES;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, valuesByGroupId.get(groupId));
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, BigintOperators.hashCode(valuesByGroupId.get(groupId)));
            }
        }
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new AddPageWork(page.getBlock(hashChannel));
    }

    @Override
    public List<Page> getBufferedPages()
    {
        // This method is left unimplemented since it is not invoked from anywhere within code.
        // Add an implementation, if needed in future
        throw new UnsupportedOperationException("BigIntGroupByHash does not support getBufferedPages");
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new GetGroupIdsWork(page.getBlock(hashChannel));
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        Block block = page.getBlock(hashChannel);
        if (block.isNull(position)) {
            return nullGroupId >= 0;
        }

        long value = BIGINT.getLong(block, position);
        long hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds.get(hashPosition);
            if (groupId == -1) {
                return false;
            }
            else if (value == values.get(hashPosition)) {
                return true;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
    }

    @Override
    public long getRawHash(int groupId)
    {
        return BigintType.hash(valuesByGroupId.get(groupId));
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    private int putIfAbsent(int position, Block block)
    {
        if (block.isNull(position)) {
            if (nullGroupId < 0) {
                // set null group id
                nullGroupId = nextGroupId++;
            }

            return nullGroupId;
        }

        long value = BIGINT.getLong(block, position);
        long hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds.get(hashPosition);
            if (groupId == -1) {
                break;
            }

            if (value == values.get(hashPosition)) {
                return groupId;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
            hashCollisions++;
        }

        return addNewGroup(hashPosition, value);
    }

    private int addNewGroup(long hashPosition, long value)
    {
        // record group id in hash
        int groupId = nextGroupId++;

        values.set(hashPosition, value);
        valuesByGroupId.set(groupId, value);
        groupIds.set(hashPosition, groupId);

        // increase capacity, if necessary
        if (needRehash()) {
            tryRehash();
        }
        return groupId;
    }

    private boolean tryRehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashCapacity) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - maxFill) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);

        int newMask = newCapacity - 1;
        LongBigArray newValues = new LongBigArray();
        newValues.ensureCapacity(newCapacity);
        IntBigArray newGroupIds = new IntBigArray(-1);
        newGroupIds.ensureCapacity(newCapacity);

        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            if (groupId == nullGroupId) {
                continue;
            }
            long value = valuesByGroupId.get(groupId);

            // find an empty slot for the address
            long hashPosition = getHashPosition(value, newMask);
            while (newGroupIds.get(hashPosition) != -1) {
                hashPosition = (hashPosition + 1) & newMask;
                hashCollisions++;
            }

            // record the mapping
            newValues.set(hashPosition, value);
            newGroupIds.set(hashPosition, groupId);
        }

        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        values = newValues;
        groupIds = newGroupIds;

        this.valuesByGroupId.ensureCapacity(maxFill);
        return true;
    }

    private boolean needRehash()
    {
        return nextGroupId >= maxFill;
    }

    private static long getHashPosition(long rawHash, int mask)
    {
        return murmurHash3(rawHash) & mask;
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

    private class AddPageWork
            implements Work<Void>
    {
        private final Block block;

        private int lastPosition;

        public AddPageWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // get the group for the current row
                putIfAbsent(lastPosition, block);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class GetGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Block block;

        private boolean finished;
        private int lastPosition;

        public GetGroupIdsWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // output the group id for this row
                BIGINT.writeLong(blockBuilder, putIfAbsent(lastPosition, block));
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(nextGroupId, blockBuilder.build());
        }
    }
}
