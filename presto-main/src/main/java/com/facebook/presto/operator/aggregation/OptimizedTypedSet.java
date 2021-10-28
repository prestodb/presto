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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.DictionaryId;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.project.SelectedPositions;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.project.SelectedPositions.positionsList;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OptimizedTypedSet
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedSet.class).instanceSize();
    private static final int ARRAY_LIST_INSTANCE_SIZE = ClassLayout.parseClass(ArrayList.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;
    private static final int INVALID_POSITION = -1;
    private static final int INITIAL_BLOCK_COUNT = 2;
    private static final SelectedPositions EMPTY_SELECTED_POSITIONS = positionsList(new int[0], 0, 0);

    private final Type elementType;
    private final int hashCapacity;
    private final int hashMask;

    private int size;  // size is the number of elements added to the TypedSet (including null).
    private Block[] blocks;   // Keeps track of the added blocks, even if the elements of the block was not inserted into the set. Array is used to get higher performance in getInsertPosition()
    private List<SelectedPositions> positionsForBlocks;  // The selected positions for the added blocks, one for each added block
    private long[] blockPositionByHash;  // Each 64-bit long is 32-bit index for blocks + 32-bit position within block
    private int currentBlockIndex = -1;  // The index into the blocks array and positionsForBlocks list

    public OptimizedTypedSet(Type elementType, int maxPositionCount)
    {
        this(elementType, INITIAL_BLOCK_COUNT, maxPositionCount);
    }

    public OptimizedTypedSet(Type elementType, int expectedBlockCount, int maxPositionCount)
    {
        checkArgument(expectedBlockCount >= 0, "expectedBlockCount must not be negative");
        checkArgument(maxPositionCount >= 0, "maxPositionCount must not be negative");

        this.elementType = requireNonNull(elementType, "elementType must not be null");
        this.hashCapacity = arraySize(maxPositionCount, FILL_RATIO);
        this.hashMask = hashCapacity - 1;

        blocks = new Block[expectedBlockCount];
        positionsForBlocks = new ArrayList<>(expectedBlockCount);
        blockPositionByHash = initializeHashTable();
    }

    /**
     * Union the set by adding the elements of the block, eliminating duplicates.
     */
    public void union(Block block)
    {
        currentBlockIndex++;
        ensureBlocksCapacity(currentBlockIndex + 1);
        blocks[currentBlockIndex] = block;

        int positionCount = block.getPositionCount();
        int[] positions = new int[positionCount];

        // Add the elements to the hash table. Since union can only increase the set size, there is no need to create a separate hashtable.
        int positionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int hashPosition = getInsertPosition(blockPositionByHash, getMaskedHash(hashPosition(elementType, block, i)), block, i);
            if (hashPosition != INVALID_POSITION) {
                // There is no need to test if adding element is successful since it's on the same hash table
                addElement(blockPositionByHash, hashPosition, block, i);
                positions[positionsIndex++] = i;
            }
        }

        getPositionsForBlocks().add(positionsList(positions, 0, positionsIndex));
        size += positionsIndex;
    }

    /**
     * Intersect the set with the elements of the block, eliminating duplicates.
     */
    public void intersect(Block block)
    {
        currentBlockIndex++;
        ensureBlocksCapacity(currentBlockIndex + 1);
        blocks[currentBlockIndex] = block;

        if (currentBlockIndex == 0) {
            // This set was an empty set, so the result set should also be an empty set.
            positionsForBlocks.add(EMPTY_SELECTED_POSITIONS);
            return;
        }

        int positionCount = block.getPositionCount();
        int[] positions = ensureCapacity(positionsForBlocks.get(currentBlockIndex - 1).getPositions(), positionCount);

        // We need to create a new hash table because the elements in the set may be removed
        long[] newBlockPositionByHash = initializeHashTable();

        int positionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int hash = getMaskedHash(hashPosition(elementType, block, i));
            int positionInBlockPositionByHash = getInsertPosition(blockPositionByHash, hash, block, i);
            if (positionInBlockPositionByHash == INVALID_POSITION) {
                // add to the hash table if it exists in blockPositionByHash
                if (addElement(newBlockPositionByHash, hash, block, i)) {
                    positions[positionsIndex++] = i;
                }
            }
        }

        blockPositionByHash = newBlockPositionByHash;
        getPositionsForBlocks().add(positionsList(positions, 0, positionsIndex));
        size = positionsIndex;

        clearPreviousBlocks();
    }

    /**
     * Add the elements of the block that do not exist in the set, eliminating duplicates, and remove all previously existing elements.
     */
    public void except(Block block)
    {
        int positionCount = block.getPositionCount();

        if (currentBlockIndex == -1) {
            // This set was an empty set. Call union() to remove duplicates.
            union(block);
            return;
        }

        currentBlockIndex++;
        ensureBlocksCapacity(currentBlockIndex + 1);
        blocks[currentBlockIndex] = block;

        int[] positions = new int[positionCount];

        // We need to create a new hash table because the elements in the set need be removed
        long[] newBlockPositionByHash = initializeHashTable();

        int positionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int hash = getMaskedHash(hashPosition(elementType, block, i));
            int positionInBlockPositionByHash = getInsertPosition(blockPositionByHash, hash, block, i);

            // add to the hash table if it does not exist in blockPositionByHash
            if (positionInBlockPositionByHash != INVALID_POSITION) {
                if (addElement(newBlockPositionByHash, hash, block, i)) {
                    positions[positionsIndex++] = i;
                }
            }
        }

        blockPositionByHash = newBlockPositionByHash;
        getPositionsForBlocks().add(positionsList(positions, 0, positionsIndex));
        size = positionsIndex;

        clearPreviousBlocks();
    }

    /**
     * Build and return the block representing this set
     */
    public Block getBlock()
    {
        if (size == 0) {
            return elementType.createBlockBuilder(null, 0).build();
        }

        if (currentBlockIndex == 0) {
            // Just one block. Return a DictionaryBlock
            Block block = blocks[currentBlockIndex];
            SelectedPositions selectedPositions = getPositionsForBlocks().get(currentBlockIndex);

            return new DictionaryBlock(
                    selectedPositions.getOffset(),
                    selectedPositions.size(),
                    block,
                    selectedPositions.getPositions(),
                    false,
                    DictionaryId.randomDictionaryId());
        }

        Block firstBlock = blocks[0];
        BlockBuilder blockBuilder = elementType.createBlockBuilder(
                null,
                size,
                toIntExact(firstBlock.getApproximateRegionLogicalSizeInBytes(0, firstBlock.getPositionCount()) / max(1, toIntExact(firstBlock.getPositionCount()))));
        for (int i = 0; i <= currentBlockIndex; i++) {
            Block block = blocks[i];
            SelectedPositions selectedPositions = getPositionsForBlocks().get(i);
            int positionCount = selectedPositions.size();

            if (!selectedPositions.isList()) {
                if (positionCount == block.getPositionCount()) {
                    return block;
                }
                else {
                    return block.getRegion(selectedPositions.getOffset(), positionCount);
                }
            }
            int[] positions = selectedPositions.getPositions();
            for (int j = 0; j < positionCount; j++) {
                // offset is always 0
                int position = positions[j];
                if (block.isNull(position)) {
                    blockBuilder.appendNull();
                }
                else {
                    elementType.appendTo(block, position, blockBuilder);
                }
            }
        }

        return blockBuilder.build();
    }

    public List<SelectedPositions> getPositionsForBlocks()
    {
        return positionsForBlocks;
    }

    public long getRetainedSizeInBytes()
    {
        long sizeInBytes = INSTANCE_SIZE + ARRAY_LIST_INSTANCE_SIZE + sizeOf(blockPositionByHash);
        for (int i = 0; i <= currentBlockIndex; i++) {
            sizeInBytes += sizeOf(positionsForBlocks.get(i).getPositions());
        }
        return sizeInBytes;
    }

    private long[] initializeHashTable()
    {
        long[] newBlockPositionByHash = new long[hashCapacity]; // Create a new hashtable
        Arrays.fill(newBlockPositionByHash, EMPTY_SLOT);
        return newBlockPositionByHash;
    }

    private void ensureBlocksCapacity(int capacity)
    {
        if (blocks == null || blocks.length < capacity) {
            blocks = Arrays.copyOf(blocks, capacity);
        }
    }

    private int getMaskedHash(long rawHash)
    {
        return (int) (rawHash & hashMask);
    }

    /**
     * Return the position in the hashtable the element at Block position should be inserted.
     *
     * @return The position of the hashtable to be inserted if the element does not exist, INVALID_POSITION otherwise.
     * @hashPosition The position into the hashtable the linear probing starts
     */
    private int getInsertPosition(long[] hashtable, int hashPosition, Block block, int position)
    {
        while (true) {
            long blockPosition = hashtable[hashPosition];
            // Doesn't have this element
            if (blockPosition == EMPTY_SLOT) {
                // does not exist
                return hashPosition;
            }

            // Already has this element
            int blockIndex = (int) ((blockPosition & 0xffff_ffff_0000_0000L) >> 32);
            int positionWithinBlock = (int) (blockPosition & 0xffff_ffff);
            if (positionEqualsPosition(elementType, blocks[blockIndex], positionWithinBlock, block, position)) {
                return INVALID_POSITION;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    /**
     * Add an element to the hash table if it's not already existed.
     *
     * @param hashtable The target hash table the element to be inserted into
     * @param hashPosition The position into the hashtable the linear probing starts
     * @return true of the element is inserted, false otherwise
     */
    private boolean addElement(long[] hashtable, int hashPosition, Block block, int position)
    {
        while (true) {
            long blockPosition = hashtable[hashPosition];

            // Doesn't have this element
            if (blockPosition == EMPTY_SLOT) {
                hashtable[hashPosition] = ((long) currentBlockIndex << 32) | position;
                return true;
            }

            // Already has this element
            int blockIndex = (int) ((blockPosition & 0xffff_ffff_0000_0000L) >> 32);
            int positionWithinBlock = (int) (blockPosition & 0xffff_ffff);
            if (positionEqualsPosition(elementType, blocks[blockIndex], positionWithinBlock, block, position)) {
                return false;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private void clearPreviousBlocks()
    {
        for (int i = 0; i < currentBlockIndex; i++) {
            positionsForBlocks.set(i, EMPTY_SELECTED_POSITIONS);
        }
    }
}
