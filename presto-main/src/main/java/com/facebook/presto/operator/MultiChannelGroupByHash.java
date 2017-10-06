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
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

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
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static it.unimi.dsi.fastutil.HashCommon.nextPowerOfTwo;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public class MultiChannelGroupByHash
        implements GroupByHash
{
    private static final int SEGMENT_SHIFT = 20;
    private static final int SEGMENT_SIZE = 1 << SEGMENT_SHIFT;
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;
    private static final long SIZE_OF_SEGMENT = sizeOfIntArray(SEGMENT_SIZE) + sizeOfByteArray(SEGMENT_SIZE) + sizeOfLongArray(SEGMENT_SIZE);

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelGroupByHash.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;
    private final List<Type> types;
    private final List<Type> hashTypes;
    private final int[] channels;

    private final PagesHashStrategy hashStrategy;
    private final List<ObjectArrayList<Block>> channelBuilders;
    private final Optional<Integer> inputHashChannel;
    private final HashGenerator hashGenerator;
    private final OptionalInt precomputedHashChannel;
    private final boolean processDictionary;
    private PageBuilder currentPageBuilder;

    private long completedPagesMemorySize;

    private int segments;
    private int hashCapacity;
    private int maxFill;
    private int mask;
    private long[][] groupAddressByHash;
    private int[][] groupIdsByHash;
    private byte[][] rawHashByHashPosition;

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
            boolean processDictionary,
            JoinCompiler joinCompiler)
    {
        this.hashTypes = ImmutableList.copyOf(requireNonNull(hashTypes, "hashTypes is null"));

        requireNonNull(joinCompiler, "joinCompiler is null");
        requireNonNull(hashChannels, "hashChannels is null");
        checkArgument(hashTypes.size() == hashChannels.length, "hashTypes and hashChannels have different sizes");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.inputHashChannel = requireNonNull(inputHashChannel, "inputHashChannel is null");
        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : this.hashTypes;
        this.channels = hashChannels.clone();

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
            this.precomputedHashChannel = OptionalInt.of(hashChannels.length);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }
        else {
            this.precomputedHashChannel = OptionalInt.empty();
        }
        this.channelBuilders = channelBuilders.build();
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(this.types, outputChannels.build());
        hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(this.channelBuilders, this.precomputedHashChannel);

        startNewPage();

        // reserve memory for the arrays
        hashCapacity = max(arraySize(expectedSize, FILL_RATIO), SEGMENT_SIZE);
        // must be power of 2
        segments = hashCapacity / SEGMENT_SIZE;
        verify(hashCapacity % SEGMENT_SIZE == 0);
        verify(segments == nextPowerOfTwo(segments));

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;

        groupAddressByHash = new long[segments][];
        groupIdsByHash = new int[segments][];
        rawHashByHashPosition = new byte[segments][];
        for (int i = 0; i < segments; i++) {
            groupAddressByHash[i] = new long[SEGMENT_SIZE];
            Arrays.fill(groupAddressByHash[i], -1);
            groupIdsByHash[i] = new int[SEGMENT_SIZE];
            rawHashByHashPosition[i] = new byte[SEGMENT_SIZE];
        }

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
        return INSTANCE_SIZE +
                (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getRetainedSizeInBytes() +
                (segments * SIZE_OF_SEGMENT) +
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
        while (groupAddressByHash[segment(hashPosition)][offset(hashPosition)] != -1) {
            if (positionEqualsCurrentRow(groupAddressByHash[segment(hashPosition)][offset(hashPosition)], hashPosition, position, page, (byte) rawHash, hashChannels)) {
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
        while (groupAddressByHash[segment(hashPosition)][offset(hashPosition)] != -1) {
            if (positionEqualsCurrentRow(groupAddressByHash[segment(hashPosition)][offset(hashPosition)], hashPosition, position, page, (byte) rawHash, channels)) {
                // found an existing slot for this key
                groupId = groupIdsByHash[segment(hashPosition)][offset(hashPosition)];

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
            BIGINT.writeLong(currentPageBuilder.getBlockBuilder(precomputedHashChannel.getAsInt()), rawHash);
        }
        currentPageBuilder.declarePosition();
        int pageIndex = channelBuilders.get(0).size() - 1;
        int pagePosition = currentPageBuilder.getPositionCount() - 1;
        long address = encodeSyntheticAddress(pageIndex, pagePosition);

        // record group id in hash
        int groupId = nextGroupId++;

        groupAddressByHash[segment(hashPosition)][offset(hashPosition)] = address;
        rawHashByHashPosition[segment(hashPosition)][offset(hashPosition)] = (byte) rawHash;
        groupIdsByHash[segment(hashPosition)][offset(hashPosition)] = groupId;
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
            currentPageBuilder = currentPageBuilder.newPageBuilderLike();
        }
        else {
            currentPageBuilder = new PageBuilder(types);
        }

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
        int newSegments = newCapacity / SEGMENT_SIZE;
        long[][] newKey = new long[newSegments][];
        byte[][] rawHashes = new byte[newSegments][];
        int[][] newValue = new int[newSegments][];

        newKey[segments - 1] = new long[SEGMENT_SIZE];
        newValue[segments - 1] = new int[SEGMENT_SIZE];
        rawHashes[segments - 1] = new byte[SEGMENT_SIZE];
        newKey[2 * segments - 1] = new long[SEGMENT_SIZE];
        newValue[2 * segments - 1] = new int[SEGMENT_SIZE];
        rawHashes[2 * segments - 1] = new byte[SEGMENT_SIZE];
        Arrays.fill(newKey[segments - 1], -1);
        Arrays.fill(newKey[2 * segments - 1], -1);

        for (int i = 0; i < segments; i++) {
            if (i != segments - 1) {
                newKey[i + segments] = new long[SEGMENT_SIZE];
                Arrays.fill(newKey[i + segments], -1);
                newValue[i + segments] = new int[SEGMENT_SIZE];
                rawHashes[i + segments] = new byte[SEGMENT_SIZE];
                if (i == 0) {
                    newKey[i] = new long[SEGMENT_SIZE];
                    newValue[i] = new int[SEGMENT_SIZE];
                    rawHashes[i] = new byte[SEGMENT_SIZE];
                }
                else {
                    newKey[i] = groupAddressByHash[i - 1];
                    newValue[i] = groupIdsByHash[i - 1];
                    rawHashes[i] = rawHashByHashPosition[i - 1];
                }
                Arrays.fill(newKey[i], -1);
            }

            for (int j = 0; j < SEGMENT_SIZE; j++) {
                // seek to the next used slot
                if (groupAddressByHash[i][j] == -1) {
                    continue;
                }

                // get the address for this slot
                long address = groupAddressByHash[i][j];

                long rawHash = hashPosition(address);
                // find an empty slot for the address
                int position = (int) getHashPosition(rawHash, newMask);
                while (newKey[segment(position)][offset(position)] != -1) {
                    position = (position + 1) & newMask;
                    hashCollisions++;
                }

                // record the mapping
                newKey[segment(position)][offset(position)] = address;
                rawHashes[segment(position)][offset(position)] = (byte) rawHash;
                newValue[segment(position)][offset(position)] = groupIdsByHash[i][j];
            }
        }

        this.mask = newMask;
        this.segments = newSegments;
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
        return channelBuilders.get(precomputedHashChannel.getAsInt()).get(sliceIndex).getLong(position, 0);
    }

    private boolean positionEqualsCurrentRow(long address, int hashPosition, int position, Page page, byte rawHash, int[] hashChannels)
    {
        if (rawHashByHashPosition[segment(hashPosition)][offset(hashPosition)] != rawHash) {
            return false;
        }
        return hashStrategy.positionEqualsRow(decodeSliceIndex(address), decodePosition(address), position, page, hashChannels);
    }

    private static int segment(int index)
    {
        return index >>> SEGMENT_SHIFT;
    }

    public static int offset(int index)
    {
        return index & SEGMENT_MASK;
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
