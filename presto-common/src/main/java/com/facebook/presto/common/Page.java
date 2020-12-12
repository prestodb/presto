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
package com.facebook.presto.common;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.DictionaryId;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.block.DictionaryId.randomDictionaryId;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Page
{
    public static final int INSTANCE_SIZE = ClassLayout.parseClass(Page.class).instanceSize();
    private static final Block[] EMPTY_BLOCKS = new Block[0];

    /**
     * Visible to give trusted classes like {@link PageBuilder} access to a constructor that doesn't
     * defensively copy the blocks
     */
    public static Page wrapBlocksWithoutCopy(int positionCount, Block[] blocks)
    {
        return new Page(false, positionCount, blocks);
    }

    private final Block[] blocks;
    private final int positionCount;
    private volatile long sizeInBytes = -1;
    private volatile long retainedSizeInBytes = -1;
    private volatile long logicalSizeInBytes = -1;

    public Page(Block... blocks)
    {
        this(true, determinePositionCount(blocks), blocks);
    }

    public Page(int positionCount)
    {
        this(false, positionCount, EMPTY_BLOCKS);
    }

    public Page(int positionCount, Block... blocks)
    {
        this(true, positionCount, blocks);
    }

    private Page(boolean blocksCopyRequired, int positionCount, Block[] blocks)
    {
        requireNonNull(blocks, "blocks is null");
        this.positionCount = positionCount;
        this.blocks = blocksCopyRequired ? blocks.clone() : blocks;
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getSizeInBytes()
    {
        long sizeInBytes = this.sizeInBytes;
        if (sizeInBytes < 0) {
            sizeInBytes = 0;
            for (Block block : blocks) {
                sizeInBytes += block.getSizeInBytes();
            }
            this.sizeInBytes = sizeInBytes;
        }
        return sizeInBytes;
    }

    public long getLogicalSizeInBytes()
    {
        long logicalSizeInBytes = this.logicalSizeInBytes;
        if (logicalSizeInBytes < 0) {
            logicalSizeInBytes = 0;
            for (Block block : blocks) {
                logicalSizeInBytes += block.getLogicalSizeInBytes();
            }
            this.logicalSizeInBytes = logicalSizeInBytes;
        }
        return logicalSizeInBytes;
    }

    /**
     * Returns the approximate logical size of the page if logicalSizeInBytes was not calculated before.
     */
    public long getApproximateLogicalSizeInBytes()
    {
        if (logicalSizeInBytes < 0) {
            long approximateLogicalSizeInBytes = 0;
            for (Block block : blocks) {
                approximateLogicalSizeInBytes += block.getApproximateRegionLogicalSizeInBytes(0, block.getPositionCount());
            }
            return approximateLogicalSizeInBytes;
        }

        return logicalSizeInBytes;
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = this.retainedSizeInBytes;
        if (retainedSizeInBytes < 0) {
            return updateRetainedSize();
        }
        return retainedSizeInBytes;
    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }

    /**
     * Gets the values at the specified position as a single element page.  The method creates independent
     * copy of the data.
     */
    public Page getSingleValuePage(int position)
    {
        Block[] singleValueBlocks = new Block[this.blocks.length];
        for (int i = 0; i < this.blocks.length; i++) {
            singleValueBlocks[i] = this.blocks[i].getSingleValueBlock(position);
        }
        return wrapBlocksWithoutCopy(1, singleValueBlocks);
    }

    public Page getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException(format("Invalid position %s and length %s in page with %s positions", positionOffset, length, positionCount));
        }

        int channelCount = getChannelCount();
        Block[] slicedBlocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            slicedBlocks[i] = blocks[i].getRegion(positionOffset, length);
        }
        return wrapBlocksWithoutCopy(length, slicedBlocks);
    }

    public Page appendColumn(Block block)
    {
        requireNonNull(block, "block is null");
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + 1);
        newBlocks[blocks.length] = block;
        return wrapBlocksWithoutCopy(positionCount, newBlocks);
    }

    public void compact()
    {
        if (getRetainedSizeInBytes() <= getSizeInBytes()) {
            return;
        }

        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            if (block instanceof DictionaryBlock) {
                continue;
            }
            // Compact the block
            blocks[i] = block.copyRegion(0, block.getPositionCount());
        }

        Map<DictionaryId, DictionaryBlockIndexes> dictionaryBlocks = getRelatedDictionaryBlocks();
        for (DictionaryBlockIndexes blockIndexes : dictionaryBlocks.values()) {
            List<DictionaryBlock> compactBlocks = compactRelatedBlocks(blockIndexes.getBlocks());
            List<Integer> indexes = blockIndexes.getIndexes();
            for (int i = 0; i < compactBlocks.size(); i++) {
                blocks[indexes.get(i)] = compactBlocks.get(i);
            }
        }

        updateRetainedSize();
    }

    private Map<DictionaryId, DictionaryBlockIndexes> getRelatedDictionaryBlocks()
    {
        Map<DictionaryId, DictionaryBlockIndexes> relatedDictionaryBlocks = new HashMap<>();

        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            if (block instanceof DictionaryBlock) {
                DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
                relatedDictionaryBlocks.computeIfAbsent(dictionaryBlock.getDictionarySourceId(), id -> new DictionaryBlockIndexes())
                        .addBlock(dictionaryBlock, i);
            }
        }
        return relatedDictionaryBlocks;
    }

    private static List<DictionaryBlock> compactRelatedBlocks(List<DictionaryBlock> blocks)
    {
        DictionaryBlock firstDictionaryBlock = blocks.get(0);
        Block dictionary = firstDictionaryBlock.getDictionary();

        int positionCount = firstDictionaryBlock.getPositionCount();
        int dictionarySize = dictionary.getPositionCount();

        // determine which dictionary entries are referenced and build a reindex for them
        int[] dictionaryPositionsToCopy = new int[min(dictionarySize, positionCount)];
        int[] remapIndex = new int[dictionarySize];
        Arrays.fill(remapIndex, -1);

        int numberOfIndexes = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = firstDictionaryBlock.getId(i);
            if (remapIndex[position] == -1) {
                dictionaryPositionsToCopy[numberOfIndexes] = position;
                remapIndex[position] = numberOfIndexes;
                numberOfIndexes++;
            }
        }

        // entire dictionary is referenced
        if (numberOfIndexes == dictionarySize) {
            return blocks;
        }

        // compact the dictionaries
        int[] newIds = getNewIds(positionCount, firstDictionaryBlock, remapIndex);
        List<DictionaryBlock> outputDictionaryBlocks = new ArrayList<>(blocks.size());
        DictionaryId newDictionaryId = randomDictionaryId();
        for (DictionaryBlock dictionaryBlock : blocks) {
            if (!firstDictionaryBlock.getDictionarySourceId().equals(dictionaryBlock.getDictionarySourceId())) {
                throw new IllegalArgumentException("dictionarySourceIds must be the same");
            }

            try {
                Block compactDictionary = dictionaryBlock.getDictionary().copyPositions(dictionaryPositionsToCopy, 0, numberOfIndexes);
                outputDictionaryBlocks.add(new DictionaryBlock(positionCount, compactDictionary, newIds, true, newDictionaryId));
            }
            catch (UnsupportedOperationException e) {
                // ignore if copy positions is not supported for the dictionary
                outputDictionaryBlocks.add(dictionaryBlock);
            }
        }
        return outputDictionaryBlocks;
    }

    private static int[] getNewIds(int positionCount, DictionaryBlock dictionaryBlock, int[] remapIndex)
    {
        int[] newIds = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            int newId = remapIndex[dictionaryBlock.getId(i)];
            if (newId == -1) {
                throw new IllegalStateException("reference to a non-existent key");
            }
            newIds[i] = newId;
        }
        return newIds;
    }

    /**
     * Returns a page that assures all data is in memory.
     * May return the same page if all page data is already in memory.
     * <p>
     * This allows streaming data sources to skip sections that are not
     * accessed in a query.
     */
    public Page getLoadedPage()
    {
        Block[] loadedBlocks = null;
        for (int i = 0; i < blocks.length; i++) {
            Block loaded = blocks[i].getLoadedBlock();
            if (loaded != blocks[i]) {
                if (loadedBlocks == null) {
                    loadedBlocks = blocks.clone();
                }
                loadedBlocks[i] = loaded;
            }
        }

        if (loadedBlocks == null) {
            return this;
        }

        return wrapBlocksWithoutCopy(positionCount, loadedBlocks);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Page{");
        builder.append("positions=").append(positionCount);
        builder.append(", channels=").append(getChannelCount());
        builder.append('}');
        builder.append("@").append(Integer.toHexString(System.identityHashCode(this)));
        return builder.toString();
    }

    private static int determinePositionCount(Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }

        return blocks[0].getPositionCount();
    }

    public Page getPositions(int[] retainedPositions, int offset, int length)
    {
        requireNonNull(retainedPositions, "retainedPositions is null");

        Block[] blocks = new Block[this.blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = this.blocks[i].getPositions(retainedPositions, offset, length);
        }
        return wrapBlocksWithoutCopy(length, blocks);
    }

    public Page copyPositions(int[] retainedPositions, int offset, int length)
    {
        requireNonNull(retainedPositions, "retainedPositions is null");

        Block[] blocks = new Block[this.blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = this.blocks[i].copyPositions(retainedPositions, offset, length);
        }
        return wrapBlocksWithoutCopy(length, blocks);
    }

    public Page extractChannel(int channel)
    {
        return wrapBlocksWithoutCopy(positionCount, new Block[]{this.blocks[channel]});
    }

    public Page extractChannels(int[] channels)
    {
        requireNonNull(channels, "channels is null");

        Block[] blocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            blocks[i] = this.blocks[channels[i]];
        }
        return wrapBlocksWithoutCopy(positionCount, blocks);
    }

    public Page prependColumn(Block column)
    {
        if (column.getPositionCount() != positionCount) {
            throw new IllegalArgumentException(String.format("Column does not have same position count (%s) as page (%s)", column.getPositionCount(), positionCount));
        }

        Block[] result = new Block[blocks.length + 1];
        result[0] = column;
        System.arraycopy(blocks, 0, result, 1, blocks.length);

        return wrapBlocksWithoutCopy(positionCount, result);
    }

    private long updateRetainedSize()
    {
        long retainedSizeInBytes = INSTANCE_SIZE + sizeOf(blocks);
        for (Block block : blocks) {
            retainedSizeInBytes += block.getRetainedSizeInBytes();
        }
        this.retainedSizeInBytes = retainedSizeInBytes;
        return retainedSizeInBytes;
    }

    private static class DictionaryBlockIndexes
    {
        private final List<DictionaryBlock> blocks = new ArrayList<>();
        private final List<Integer> indexes = new ArrayList<>();

        public void addBlock(DictionaryBlock block, int index)
        {
            blocks.add(block);
            indexes.add(index);
        }

        public List<DictionaryBlock> getBlocks()
        {
            return blocks;
        }

        public List<Integer> getIndexes()
        {
            return indexes;
        }
    }
}
