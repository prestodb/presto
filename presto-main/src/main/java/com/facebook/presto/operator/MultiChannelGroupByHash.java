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
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

// This implementation assumes arrays used in the hash are always a power of 2
public class MultiChannelGroupByHash
        implements GroupByHash
{
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler();

    private static final float FILL_RATIO = 0.9f;
    private final List<Type> types;
    private final int[] channels;

    private final PagesHashStrategy hashStrategy;
    private final List<ObjectArrayList<Block>> channelBuilders;
    private final HashGenerator hashGenerator;
    private final Optional<Integer> precomputedHashChannel;
    private final int maskChannel;
    private PageBuilder currentPageBuilder;

    private long completedPagesMemorySize;

    private int maxFill;
    private int mask;
    private long[] groupAddressByHash;
    private int[] groupIdsByHash;

    private final LongBigArray groupAddressByGroupId;

    private int nextGroupId;

    public MultiChannelGroupByHash(List<? extends Type> hashTypes, int[] hashChannels, Optional<Integer> maskChannel, Optional<Integer> inputHashChannel, int expectedSize)
    {
        checkNotNull(hashTypes, "hashTypes is null");
        checkArgument(hashTypes.size() == hashChannels.length, "hashTypes and hashChannels have different sizes");
        checkNotNull(inputHashChannel, "inputHashChannel is null");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : ImmutableList.copyOf(hashTypes);
        this.channels = checkNotNull(hashChannels, "hashChannels is null").clone();
        this.maskChannel = checkNotNull(maskChannel, "maskChannel is null").orElse(-1);
        this.hashGenerator = inputHashChannel.isPresent() ? new PrecomputedHashGenerator(inputHashChannel.get()) : new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);

        // For each hashed channel, create an appendable list to hold the blocks (builders).  As we
        // add new values we append them to the existing block builder until it fills up and then
        // we add a new block builder to each list.
        ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
        ImmutableList.Builder<ObjectArrayList<Block>> channelBuilders = ImmutableList.builder();
        for (int i = 0; i < hashChannels.length; i++) {
            outputChannels.add(i);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }
        if (inputHashChannel.isPresent()) {
            this.precomputedHashChannel = Optional.of(hashChannels.length);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }
        else {
            this.precomputedHashChannel = Optional.empty();
        }
        this.channelBuilders = channelBuilders.build();
        PagesHashStrategyFactory pagesHashStrategyFactory = JOIN_COMPILER.compilePagesHashStrategyFactory(this.types, outputChannels.build());
        hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(this.channelBuilders, this.precomputedHashChannel);

        startNewPage();

        // reserve memory for the arrays
        int hashSize = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashSize);
        mask = hashSize - 1;
        groupAddressByHash = new long[hashSize];
        Arrays.fill(groupAddressByHash, -1);

        groupIdsByHash = new int[hashSize];

        groupAddressByGroupId = new LongBigArray();
        groupAddressByGroupId.ensureCapacity(maxFill);
    }

    @Override
    public long getEstimatedSize()
    {
        return (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getRetainedSizeInBytes() +
                sizeOf(groupAddressByHash) +
                sizeOf(groupIdsByHash) +
                groupAddressByGroupId.sizeOf();
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long address = groupAddressByGroupId.get(groupId);
        int blockIndex = decodeSliceIndex(address);
        int position = decodePosition(address);
        hashStrategy.appendTo(blockIndex, position, pageBuilder, outputChannelOffset);
    }

    @Override
    public void addPage(Page page)
    {
        Block[] hashBlocks = extractHashColumns(page);

        Block maskBlock = null;
        if (maskChannel >= 0) {
            maskBlock = page.getBlock(maskChannel);
        }

        // get the group id for each position
        int positionCount = page.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            // skip masked rows
            if (maskBlock != null && !BOOLEAN.getBoolean(maskBlock, position)) {
                continue;
            }

            // get the group for the current row
            putIfAbsent(position, page, hashBlocks);
        }
    }

    @Override
    public GroupByIdBlock getGroupIds(Page page)
    {
        int positionCount = page.getPositionCount();

        // we know the exact size required for the block
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(positionCount);

        Block maskBlock = null;
        if (maskChannel >= 0) {
            maskBlock = page.getBlock(maskChannel);
        }

        // extract the hash columns
        Block[] hashBlocks = extractHashColumns(page);

        // get the group id for each position
        for (int position = 0; position < positionCount; position++) {
            // skip masked rows
            if (maskBlock != null && !BOOLEAN.getBoolean(maskBlock, position)) {
                blockBuilder.appendNull();
                continue;
            }

            // get the group for the current row
            int groupId = putIfAbsent(position, page, hashBlocks);

            // output the group id for this row
            BIGINT.writeLong(blockBuilder, groupId);
        }
        return new GroupByIdBlock(nextGroupId, blockBuilder.build());
    }

    @Override
    public boolean contains(int position, Page page)
    {
        int rawHash = hashStrategy.hashRow(position, page.getBlocks());
        int hashPosition = getHashPosition(rawHash, mask);

        // look for a slot containing this key
        while (groupAddressByHash[hashPosition] != -1) {
            long address = groupAddressByHash[hashPosition];
            if (hashStrategy.positionEqualsRow(decodeSliceIndex(address), decodePosition(address), position, page.getBlocks())) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return false;
    }

    @Override
    public int putIfAbsent(int position, Page page)
    {
        return putIfAbsent(position, page, extractHashColumns(page));
    }

    private int putIfAbsent(int position, Page page, Block[] hashBlocks)
    {
        int rawHash = hashGenerator.hashPosition(position, page);
        int hashPosition = getHashPosition(rawHash, mask);

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (groupAddressByHash[hashPosition] != -1) {
            long address = groupAddressByHash[hashPosition];
            if (positionEqualsCurrentRow(decodeSliceIndex(address), decodePosition(address), position, hashBlocks)) {
                // found an existing slot for this key
                groupId = groupIdsByHash[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, position, page, rawHash);
        }
        return groupId;
    }

    private int addNewGroup(int hashPosition, int position, Page page, int rawHash)
    {
        // add the row to the open page
        Block[] blocks = page.getBlocks();
        for (int i = 0; i < channels.length; i++) {
            int hashChannel = channels[i];
            Type type = types.get(i);
            type.appendTo(blocks[hashChannel], position, currentPageBuilder.getBlockBuilder(i));
        }
        if (precomputedHashChannel.isPresent()) {
            BIGINT.writeLong(currentPageBuilder.getBlockBuilder(precomputedHashChannel.get()), rawHash);
        }
        currentPageBuilder.declarePosition();
        int pageIndex = channelBuilders.get(0).size() - 1;
        int pagePosition = currentPageBuilder.getPositionCount() - 1;
        long address = encodeSyntheticAddress(pageIndex, pagePosition);

        // record group id in hash
        int groupId = nextGroupId++;

        groupAddressByHash[hashPosition] = address;
        groupIdsByHash[hashPosition] = groupId;
        groupAddressByGroupId.set(groupId, address);

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
            completedPagesMemorySize += currentPageBuilder.getRetainedSizeInBytes();
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
            while (groupAddressByHash[oldIndex] == -1) {
                oldIndex++;
            }

            // get the address for this slot
            long address = groupAddressByHash[oldIndex];

            // find an empty slot for the address
            int pos = getHashPosition(hashPosition(address), newMask);
            while (newKey[pos] != -1) {
                pos = (pos + 1) & newMask;
            }

            // record the mapping
            newKey[pos] = address;
            newValue[pos] = groupIdsByHash[oldIndex];
            oldIndex++;
        }

        this.mask = newMask;
        this.maxFill = calculateMaxFill(newSize);
        this.groupAddressByHash = newKey;
        this.groupIdsByHash = newValue;
        groupAddressByGroupId.ensureCapacity(maxFill);
    }

    private Block[] extractHashColumns(Page page)
    {
        Block[] hashBlocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            hashBlocks[i] = page.getBlock(channels[i]);
        }
        return hashBlocks;
    }

    private int hashPosition(long sliceAddress)
    {
        int sliceIndex = decodeSliceIndex(sliceAddress);
        int position = decodePosition(sliceAddress);
        if (precomputedHashChannel.isPresent()) {
            return getRawHash(sliceIndex, position);
        }
        return hashStrategy.hashPosition(sliceIndex, position);
    }

    private int getRawHash(int sliceIndex, int position)
    {
        return (int) channelBuilders.get(precomputedHashChannel.get()).get(sliceIndex).getLong(position, 0);
    }

    private boolean positionEqualsCurrentRow(int sliceIndex, int slicePosition, int position, Block[] blocks)
    {
        return hashStrategy.positionEqualsRow(sliceIndex, slicePosition, position, blocks);
    }

    private static int getHashPosition(int rawHash, int mask)
    {
        return murmurHash3(rawHash) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }
}
