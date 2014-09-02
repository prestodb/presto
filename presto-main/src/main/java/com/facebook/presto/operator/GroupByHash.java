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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
    private static final float FILL_RATIO = 0.75f;
    private final List<Type> types;
    private final int[] channels;
    private final int inputHashChannel;
    private final int outputHashChannel;

    private final List<ObjectArrayList<Block>> channelBuilders;
    private PageBuilder currentPageBuilder;

    private long completedPagesMemorySize;

    private int maxFill;
    private int mask;
    private long[] key;
    private int[] value;

    private final LongBigArray groupAddress;

    private int nextGroupId;

    /**
     * @param types List of types of channels to group by
     * @param channels Corresponding channels to group by
     * @param inputHashChannel Channel containing the hash of {@code channels}
     * @param expectedSize Number of expected groups
     */
    public GroupByHash(List<? extends Type> types, int[] channels, int inputHashChannel, int expectedSize)
    {
        checkNotNull(types, "types is null");
        checkArgument(!types.isEmpty(), "empty types");
        checkArgument(types.size() == channels.length, "types and channels have different sizes");
        checkArgument(inputHashChannel >= 0, "invalid input hash column");

        this.channelBuilders = getChannelBuilders(channels);
        // Additional BIGINT channel for storing hash
        this.types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        this.channels = checkNotNull(channels, "channels is null").clone();

        this.inputHashChannel = inputHashChannel;
        this.outputHashChannel = channels.length;   // Output hash goes in last channel

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

    /**
     * Append the values of groupId to pageBuilder. The last block is the rawHash of the groupId
     */
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long address = groupAddress.get(groupId);
        int blockIndex = decodeSliceIndex(address);
        int position = decodePosition(address);
        for (int i = 0; i < channels.length; i++) {
            Type type = types.get(i);
            Block block = channelBuilders.get(i).get(blockIndex);
            type.appendTo(block, position, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
        BIGINT.appendTo(channelBuilders.get(outputHashChannel).get(blockIndex), position, pageBuilder.getBlockBuilder(outputHashChannel));
    }

    /**
     * Get the group id for each row of the specified page
     */
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
        Block blockContainingHash = page.getBlock(inputHashChannel);

        // get the group id for each position
        for (int position = 0; position < page.getPositionCount(); position++) {
            // get the group for the current row
            int groupId = putIfAbsent(position, blockContainingHash, blocks);

            // output the group id for this row
            BIGINT.writeLong(blockBuilder, groupId);
        }

        Block block = blockBuilder.build();
        return new GroupByIdBlock(nextGroupId, block);
    }

    /**
     * Inspect values at position in blocks
     *
     * @param position position in the blocks to inspect
     * @param hashBlock Block containing the rawHash for {@code blocks}
     * @param blocks Blocks containing values to groupBy
     * @return true if group is already present, false otherwise
     */
    public boolean contains(int position, Block hashBlock, Block... blocks)
    {
        int rawHash = (int) BIGINT.getLong(hashBlock, position);
        int hashPosition = ((int) Murmur3.hash64(rawHash)) & mask;

        // look for a slot containing this key
        while (key[hashPosition] != -1) {
            long address = key[hashPosition];
            if (positionEqualsCurrentRow(decodeSliceIndex(address), decodePosition(address), position, rawHash, blocks)) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return false;
    }

    /**
     * Inspect the values at position and add a new group if necessary
     */
    public int putIfAbsent(int position, final Block hashBlock, Block... blocks)
    {
        int rawHash = (int) BIGINT.getLong(hashBlock, position);
        int hashPosition = ((int) Murmur3.hash64(rawHash)) & mask;

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (key[hashPosition] != -1) {
            long address = key[hashPosition];
            if (positionEqualsCurrentRow(decodeSliceIndex(address), decodePosition(address), position, rawHash, blocks)) {
                // found an existing slot for this key
                groupId = value[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, position, blocks, rawHash);
        }
        return groupId;
    }

    private static List<ObjectArrayList<Block>> getChannelBuilders(int[] channels)
    {
        // For each hashed channel, create an appendable list to hold the blocks (builders).  As we
        // add new values we append them to the existing block builder until it fills up and then
        // we add a new block builder to each list.
        ImmutableList.Builder<Integer> hashChannels = ImmutableList.builder();
        ImmutableList.Builder<ObjectArrayList<Block>> channelBuilders = ImmutableList.builder();
        for (int i = 0; i < channels.length; i++) {
            hashChannels.add(i);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }
        channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0)); // additional channel for storing hash
        return channelBuilders.build();
    }

    private int addNewGroup(int hashPosition, int position, Block[] blocks, int rawHash)
    {
        // add the row to the open page
        for (int i = 0; i < blocks.length; i++) {
            Type type = types.get(i);
            type.appendTo(blocks[i], position, currentPageBuilder.getBlockBuilder(i));
        }
        // write hash
        BIGINT.writeLong(currentPageBuilder.getBlockBuilder(outputHashChannel), rawHash);

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
        Block hashBlock = channelBuilders.get(outputHashChannel).get(sliceIndex);
        return (int) BigintType.BIGINT.getLong(hashBlock, position);
    }

    private boolean positionEqualsCurrentRow(int sliceIndex, int slicePosition, int position, int rawHash, Block[] blocks)
    {
        if ((int) BIGINT.getLong(channelBuilders.get(outputHashChannel).get(sliceIndex), slicePosition) != rawHash) {
            return false;
        }

        for (int hashChannel = 0; hashChannel < channels.length; hashChannel++) {
            Type type = types.get(hashChannel);
            Block leftBlock = channelBuilders.get(hashChannel).get(sliceIndex);
            Block rightBlock = blocks[hashChannel];
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, slicePosition, rightBlock, position)) {
                return false;
            }
        }
        return true;
    }
}
