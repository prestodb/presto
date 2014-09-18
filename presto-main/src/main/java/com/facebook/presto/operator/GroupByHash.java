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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Murmur3;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

// This implementation assumes arrays used in the hash are always a power of 2
public class GroupByHash
{
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler();

    private static final float FILL_RATIO = 0.75f;
    private final List<Type> types;
    private final int[] channels;

    private final PagesHashStrategy hashStrategy;
    private final List<ObjectArrayList<Block>> channelBuilders;
    private PageBuilder currentPageBuilder;

    private long completedPagesMemorySize;

    private int maxFill;
    private int mask;
    private long[] key;
    private int[] value;

    private final LongBigArray groupAddress;

    private int nextGroupId;

    public GroupByHash(List<? extends Type> types, int[] channels, int expectedSize)
    {
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.channels = checkNotNull(channels, "channels is null").clone();
        checkArgument(types.size() == channels.length, "types and channels have different sizes");

        // For each hashed channel, create an appendable list to hold the blocks (builders).  As we
        // add new values we append them to the existing block builder until it fills up and then
        // we add a new block builder to each list.
        ImmutableList.Builder<Integer> hashChannels = ImmutableList.builder();
        ImmutableList.Builder<ObjectArrayList<Block>> channelBuilders = ImmutableList.builder();
        for (int i = 0; i < channels.length; i++) {
            hashChannels.add(i);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }

        this.channelBuilders = channelBuilders.build();
        PagesHashStrategyFactory pagesHashStrategyFactory = JOIN_COMPILER.compilePagesHashStrategyFactory(this.types, hashChannels.build());
        hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(this.channelBuilders);

        startNewPage();

        // reserve memory for the arrays
        int hashSize = arraySize(expectedSize, FILL_RATIO);

        maxFill = maxFill(hashSize, FILL_RATIO);
        mask = hashSize - 1;
        key = new long[hashSize];
        Arrays.fill(key, -1);

        value = new int[hashSize];

        groupAddress = new LongBigArray();
        groupAddress.ensureCapacity(maxFill);
    }

    public long getEstimatedSize()
    {
        return (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getSizeInBytes() +
                sizeOf(key) +
                sizeOf(value) +
                groupAddress.sizeOf();
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getGroupCount()
    {
        return nextGroupId;
    }

    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long address = groupAddress.get(groupId);
        int blockIndex = decodeSliceIndex(address);
        int position = decodePosition(address);
        hashStrategy.appendTo(blockIndex, position, pageBuilder, outputChannelOffset);
    }

    public GroupByIdBlock getGroupIds(Page page)
    {
        int positionCount = page.getPositionCount();

        // we know the exact size required for the block
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(positionCount);

        // extract the hash columns
        Block[] blocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            blocks[i] = page.getBlock(channels[i]);
        }

        // get the group id for each position
        for (int position = 0; position < page.getPositionCount(); position++) {
            // get the group for the current row
            int groupId = putIfAbsent(position, blocks);

            // output the group id for this row
            BIGINT.writeLong(blockBuilder, groupId);
        }

        Block block = blockBuilder.build();
        return new GroupByIdBlock(nextGroupId, block);
    }

    public boolean contains(int position, Block... blocks)
    {
        int hashPosition = ((int) Murmur3.hash64(hashStrategy.hashRow(position, blocks))) & mask;

        // look for a slot containing this key
        while (key[hashPosition] != -1) {
            long address = key[hashPosition];
            if (positionEqualsCurrentRow(decodeSliceIndex(address), decodePosition(address), position, blocks)) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return false;
    }

    public int putIfAbsent(int position, Block... blocks)
    {
        int hashPosition = ((int) Murmur3.hash64(hashStrategy.hashRow(position, blocks))) & mask;

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (key[hashPosition] != -1) {
            long address = key[hashPosition];
            if (positionEqualsCurrentRow(decodeSliceIndex(address), decodePosition(address), position, blocks)) {
                // found an existing slot for this key
                groupId = value[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, position, blocks);
        }
        return groupId;
    }

    private int addNewGroup(int hashPosition, int position, Block[] blocks)
    {
        // add the row to the open page
        for (int i = 0; i < blocks.length; i++) {
            Type type = types.get(i);
            type.appendTo(blocks[i], position, currentPageBuilder.getBlockBuilder(i));
        }
        currentPageBuilder.declarePosition();
        int pageIndex = channelBuilders.get(0).size() - 1;
        int pagePosition = currentPageBuilder.getPositionCount() - 1;
        long address = encodeSyntheticAddress(pageIndex, pagePosition);

        // record group id in hash
        int groupId = nextGroupId++;

        key[hashPosition] = address;
        value[hashPosition] = groupId;
        groupAddress.set(groupId, address);

        // create new page builder if this page is full
        if (currentPageBuilder.isFull()) {
            startNewPage();
        }

        // increase capacity, if necessary
        if (nextGroupId >= maxFill) {
            rehash(maxFill * 2);
        }
        return groupId;
    }

    private void startNewPage()
    {
        if (currentPageBuilder != null) {
            completedPagesMemorySize += currentPageBuilder.getSizeInBytes();
        }

        currentPageBuilder = new PageBuilder(types);
        for (int i = 0; i < types.size(); i++) {
            channelBuilders.get(i).add(currentPageBuilder.getBlockBuilder(i));
        }
    }

    private void rehash(int size)
    {
        int newSize = arraySize(size + 1, FILL_RATIO);

        int newMask = newSize - 1;
        long[] newKey = new long[newSize];
        Arrays.fill(newKey, -1);
        int[] newValue = new int[newSize];

        int oldIndex = 0;
        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            // seek to the next used slot
            while (key[oldIndex] == -1) {
                oldIndex++;
            }

            // get the address for this slot
            long address = key[oldIndex];

            // find an empty slot for the address
            int pos = ((int) Murmur3.hash64(hashPosition(address))) & newMask;
            while (newKey[pos] != -1) {
                pos = (pos + 1) & newMask;
            }

            // record the mapping
            newKey[pos] = address;
            newValue[pos] = value[oldIndex];
            oldIndex++;
        }

        this.mask = newMask;
        this.maxFill = maxFill(newSize, FILL_RATIO);
        this.key = newKey;
        this.value = newValue;
        groupAddress.ensureCapacity(maxFill);
    }

    private int hashPosition(long sliceAddress)
    {
        int sliceIndex = decodeSliceIndex(sliceAddress);
        int position = decodePosition(sliceAddress);
        return hashStrategy.hashPosition(sliceIndex, position);
    }

    private boolean positionEqualsCurrentRow(int sliceIndex, int slicePosition, int position, Block[] blocks)
    {
        return hashStrategy.positionEqualsRow(sliceIndex, slicePosition, position, blocks);
    }
}
