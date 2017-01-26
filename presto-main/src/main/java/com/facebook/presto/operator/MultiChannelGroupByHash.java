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

import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import static com.facebook.presto.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public class MultiChannelGroupByHash
        implements GroupByHash
{
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler();

    private static final float FILL_RATIO = 0.75f;
    private final List<Type> types;
    private final List<Type> hashTypes;
    private final int[] channels;

    private final PagesHashStrategy hashStrategy;
    private final List<ObjectArrayList<Block>> channelBuilders;
    private final Optional<Integer> inputHashChannel;
    private final HashGenerator hashGenerator;
    private final Optional<Integer> precomputedHashChannel;
    private final boolean processDictionary;
    private PageBuilder currentPageBuilder;

    private long completedPagesMemorySize;

    private int hashCapacity;
    private int maxFill;
    private int mask;
    private long[] groupAddressByHash;
    private int[] groupIdsByHash;
    private byte[] rawHashByHashPosition;

    private final LongBigArray groupAddressByGroupId;

    private int nextGroupId;
    private DictionaryLookBack dictionaryLookBack;
    private long hashCollisions;
    private double expectedHashCollisions;

    public MultiChannelGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary)
    {
        this.hashTypes = ImmutableList.copyOf(requireNonNull(hashTypes, "hashTypes is null"));

        checkArgument(hashTypes.size() == hashChannels.length, "hashTypes and hashChannels have different sizes");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.inputHashChannel = requireNonNull(inputHashChannel, "inputHashChannel is null");
        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : this.hashTypes;
        this.channels = requireNonNull(hashChannels, "hashChannels is null").clone();

        this.hashGenerator = inputHashChannel.isPresent() ? new PrecomputedHashGenerator(inputHashChannel.get()) : new InterpretedHashGenerator(this.hashTypes, hashChannels);
        this.processDictionary = processDictionary;

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
        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        groupAddressByHash = new long[hashCapacity];
        Arrays.fill(groupAddressByHash, -1);

        rawHashByHashPosition = new byte[hashCapacity];

        groupIdsByHash = new int[hashCapacity];

        groupAddressByGroupId = new LongBigArray();
        groupAddressByGroupId.ensureCapacity(maxFill);
    }

    @Override
    public long getRawHash(int groupId)
    {
        long address = groupAddressByGroupId.get(groupId);
        int blockIndex = decodeSliceIndex(address);
        int position = decodePosition(address);
        return hashStrategy.hashPosition(blockIndex, position);
    }

    @Override
    public long getEstimatedSize()
    {
        return (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getRetainedSizeInBytes() +
                sizeOf(groupAddressByHash) +
                sizeOf(groupIdsByHash) +
                groupAddressByGroupId.sizeOf() +
                sizeOf(rawHashByHashPosition);
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
        if (canProcessDictionary(page)) {
            addDictionaryPage(page);
            return;
        }

        // get the group id for each position
        int positionCount = page.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            // get the group for the current row
            putIfAbsent(position, page);
        }
    }

    @Override
    public GroupByIdBlock getGroupIds(Page page)
    {
        int positionCount = page.getPositionCount();

        // we know the exact size required for the block
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(positionCount);

        if (canProcessDictionary(page)) {
            Block groupIds = processDictionary(page);
            return new GroupByIdBlock(nextGroupId, groupIds);
        }

        // get the group id for each position
        for (int position = 0; position < positionCount; position++) {
            // get the group for the current row
            int groupId = putIfAbsent(position, page);

            // output the group id for this row
            BIGINT.writeLong(blockBuilder, groupId);
        }
        return new GroupByIdBlock(nextGroupId, blockBuilder.build());
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        long rawHash = hashStrategy.hashRow(position, page);
        int hashPosition = (int) getHashPosition(rawHash, mask);

        // look for a slot containing this key
        while (groupAddressByHash[hashPosition] != -1) {
            if (positionEqualsCurrentRow(groupAddressByHash[hashPosition], hashPosition, position, page, (byte) rawHash, hashChannels)) {
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
        long rawHash = hashGenerator.hashPosition(position, page);
        return putIfAbsent(position, page, rawHash);
    }

    private int putIfAbsent(int position, Page page, long rawHash)
    {
        int hashPosition = (int) getHashPosition(rawHash, mask);

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (groupAddressByHash[hashPosition] != -1) {
            if (positionEqualsCurrentRow(groupAddressByHash[hashPosition], hashPosition, position, page, (byte) rawHash, channels)) {
                // found an existing slot for this key
                groupId = groupIdsByHash[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
            hashCollisions++;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, position, page, rawHash);
        }
        return groupId;
    }

    private int addNewGroup(int hashPosition, int position, Page page, long rawHash)
    {
        // add the row to the open page
        for (int i = 0; i < channels.length; i++) {
            int hashChannel = channels[i];
            Type type = types.get(i);
            type.appendTo(page.getBlock(hashChannel), position, currentPageBuilder.getBlockBuilder(i));
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
        rawHashByHashPosition[hashPosition] = (byte) rawHash;
        groupIdsByHash[hashPosition] = groupId;
        groupAddressByGroupId.set(groupId, address);

        // create new page builder if this page is full
        if (currentPageBuilder.isFull()) {
            startNewPage();
        }

        // increase capacity, if necessary
        if (nextGroupId >= maxFill) {
            rehash();
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

    private void rehash()
    {
        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);

        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;

        int newMask = newCapacity - 1;
        long[] newKey = new long[newCapacity];
        byte[] rawHashes = new byte[newCapacity];
        Arrays.fill(newKey, -1);
        int[] newValue = new int[newCapacity];

        int oldIndex = 0;
        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            // seek to the next used slot
            while (groupAddressByHash[oldIndex] == -1) {
                oldIndex++;
            }

            // get the address for this slot
            long address = groupAddressByHash[oldIndex];

            long rawHash = hashPosition(address);
            // find an empty slot for the address
            int pos = (int) getHashPosition(rawHash, newMask);
            while (newKey[pos] != -1) {
                pos = (pos + 1) & newMask;
                hashCollisions++;
            }

            // record the mapping
            newKey[pos] = address;
            rawHashes[pos] = (byte) rawHash;
            newValue[pos] = groupIdsByHash[oldIndex];
            oldIndex++;
        }

        this.mask = newMask;
        this.hashCapacity = newCapacity;
        this.maxFill = calculateMaxFill(newCapacity);
        this.groupAddressByHash = newKey;
        this.rawHashByHashPosition = rawHashes;
        this.groupIdsByHash = newValue;
        groupAddressByGroupId.ensureCapacity(maxFill);
    }

    private long hashPosition(long sliceAddress)
    {
        int sliceIndex = decodeSliceIndex(sliceAddress);
        int position = decodePosition(sliceAddress);
        if (precomputedHashChannel.isPresent()) {
            return getRawHash(sliceIndex, position);
        }
        return hashStrategy.hashPosition(sliceIndex, position);
    }

    private long getRawHash(int sliceIndex, int position)
    {
        return channelBuilders.get(precomputedHashChannel.get()).get(sliceIndex).getLong(position, 0);
    }

    private boolean positionEqualsCurrentRow(long address, int hashPosition, int position, Page page, byte rawHash, int[] hashChannels)
    {
        if (rawHashByHashPosition[hashPosition] != rawHash) {
            return false;
        }
        return hashStrategy.positionEqualsRow(decodeSliceIndex(address), decodePosition(address), position, page, hashChannels);
    }

    private static long getHashPosition(long rawHash, int mask)
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

    private void addDictionaryPage(Page page)
    {
        verify(canProcessDictionary(page), "invalid call to addDictionaryPage");

        DictionaryBlock dictionaryBlock = (DictionaryBlock) page.getBlock(channels[0]);
        updateDictionaryLookBack(dictionaryBlock.getDictionary());
        Page dictionaryPage = createPageWithExtractedDictionary(page);

        for (int i = 0; i < page.getPositionCount(); i++) {
            int positionInDictionary = dictionaryBlock.getId(i);
            getGroupId(hashGenerator, dictionaryPage, positionInDictionary);
        }
    }

    private void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    private Block processDictionary(Page page)
    {
        verify(canProcessDictionary(page), "invalid call to processDictionary");

        DictionaryBlock dictionaryBlock = (DictionaryBlock) page.getBlock(channels[0]);
        updateDictionaryLookBack(dictionaryBlock.getDictionary());
        Page dictionaryPage = createPageWithExtractedDictionary(page);

        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
        for (int i = 0; i < page.getPositionCount(); i++) {
            int positionInDictionary = dictionaryBlock.getId(i);
            int groupId = getGroupId(hashGenerator, dictionaryPage, positionInDictionary);
            BIGINT.writeLong(blockBuilder, groupId);
        }

        verify(blockBuilder.getPositionCount() == page.getPositionCount(), "invalid position count");
        return blockBuilder.build();
    }

    // For a page that contains DictionaryBlocks, create a new page in which
    // the dictionaries from the DictionaryBlocks are extracted into the corresponding channels
    // From Page(DictionaryBlock1, DictionaryBlock2) create new page with Page(dictionary1, dictionary2)
    private Page createPageWithExtractedDictionary(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        Block dictionary = ((DictionaryBlock) page.getBlock(channels[0])).getDictionary();

        // extract data dictionary
        blocks[channels[0]] = dictionary;

        // extract hash dictionary
        if (inputHashChannel.isPresent()) {
            blocks[inputHashChannel.get()] = ((DictionaryBlock) page.getBlock(inputHashChannel.get())).getDictionary();
        }

        return new Page(dictionary.getPositionCount(), blocks);
    }

    private boolean canProcessDictionary(Page page)
    {
        boolean processDictionary = this.processDictionary &&
                channels.length == 1 &&
                page.getBlock(channels[0]) instanceof DictionaryBlock;

        if (processDictionary && inputHashChannel.isPresent()) {
            Block inputHashBlock = page.getBlock(inputHashChannel.get());
            DictionaryBlock inputDataBlock = (DictionaryBlock) page.getBlock(channels[0]);

            verify(inputHashBlock instanceof DictionaryBlock, "data channel is dictionary encoded but hash channel is not");
            verify(((DictionaryBlock) inputHashBlock).getDictionarySourceId().equals(inputDataBlock.getDictionarySourceId()),
                    "dictionarySourceIds of data block and hash block do not match");
        }
        return processDictionary;
    }

    private int getGroupId(HashGenerator hashGenerator, Page page, int positionInDictionary)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = putIfAbsent(positionInDictionary, page, hashGenerator.hashPosition(positionInDictionary, page));
        dictionaryLookBack.setProcessed(positionInDictionary, groupId);
        return groupId;
    }

    private static final class DictionaryLookBack
    {
        private final Block dictionary;
        private final int[] processed;

        public DictionaryLookBack(Block dictionary)
        {
            this.dictionary = dictionary;
            this.processed = new int[dictionary.getPositionCount()];
            Arrays.fill(processed, -1);
        }

        public Block getDictionary()
        {
            return dictionary;
        }

        public int getGroupId(int position)
        {
            return processed[position];
        }

        public boolean isProcessed(int position)
        {
            return processed[position] != -1;
        }

        public void setProcessed(int position, int groupId)
        {
            processed[position] = groupId;
        }
    }
}
