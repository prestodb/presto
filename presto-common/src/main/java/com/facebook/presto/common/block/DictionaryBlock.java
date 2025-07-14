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
package com.facebook.presto.common.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidPosition;
import static com.facebook.presto.common.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static com.facebook.presto.common.block.DictionaryId.randomDictionaryId;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A dictionary holds positionCount values of arbitrary types. Usually some of these values are repeated,
 * and the block wraps an underlying delegate block with fewer or no repeated values.
 * This delegate block is called the "dictionary".
 * The ids array contains positionCount indexes into the underlying delegate block.
 * When value N is requested from this block instead of returning the value directly,
 * it looks up the index of value N at ids[N]; then it returns the value in dictionary[ids[N]].
 * This compresses data when the same value repeats at multiple locations.
 *
 * Not every id in the ids array is a valid position in the block.
 * Specify an offset in the ids array to indicate that IDs are only stored from that position forward.
 * If the ids array is longer than offset+positionCount, then extra values to the right are not valid.
 * That is, IDs are stored in a range of the array from offset to offset+positionCount-1 (inclusive).
 */
public class DictionaryBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryBlock.class).instanceSize() + ClassLayout.parseClass(DictionaryId.class).instanceSize();
    private static final int NULL_NOT_FOUND = -1;

    private final int positionCount;
    private final Block dictionary;
    private final int idsOffset;
    private final int[] ids;
    private final long retainedSizeInBytes;
    private final DictionaryId dictionarySourceId;

    private volatile long sizeInBytes = -1;
    private volatile long logicalSizeInBytes = -1;
    private volatile int uniqueIds = -1;

    public DictionaryBlock(Block dictionary, int[] ids)
    {
        this(requireNonNull(ids, "ids is null").length, dictionary, ids);
    }

    public DictionaryBlock(int positionCount, Block dictionary, int[] ids)
    {
        this(0, positionCount, dictionary, ids, false, randomDictionaryId());
    }

    public DictionaryBlock(int positionCount, Block dictionary, int[] ids, DictionaryId dictionaryId)
    {
        this(0, positionCount, dictionary, ids, false, dictionaryId);
    }

    public DictionaryBlock(int positionCount, Block dictionary, int[] ids, boolean dictionaryIsCompacted)
    {
        this(0, positionCount, dictionary, ids, dictionaryIsCompacted, randomDictionaryId());
    }

    public DictionaryBlock(int positionCount, Block dictionary, int[] ids, boolean dictionaryIsCompacted, DictionaryId dictionarySourceId)
    {
        this(0, positionCount, dictionary, ids, dictionaryIsCompacted, dictionarySourceId);
    }

    public DictionaryBlock(int idsOffset, int positionCount, Block dictionary, int[] ids, boolean dictionaryIsCompacted, DictionaryId dictionarySourceId)
    {
        requireNonNull(dictionary, "dictionary is null");
        requireNonNull(ids, "ids is null");

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        this.idsOffset = idsOffset;
        if (ids.length - idsOffset < positionCount) {
            throw new IllegalArgumentException("ids length is less than positionCount");
        }

        this.positionCount = positionCount;
        this.dictionary = dictionary;
        this.ids = ids;
        this.dictionarySourceId = requireNonNull(dictionarySourceId, "dictionarySourceId is null");
        this.retainedSizeInBytes = INSTANCE_SIZE + dictionary.getRetainedSizeInBytes() + sizeOf(ids);

        if (dictionaryIsCompacted) {
            this.sizeInBytes = dictionary.getSizeInBytes() + (Integer.BYTES * (long) positionCount);
            this.uniqueIds = dictionary.getPositionCount();
        }
    }

    @Override
    public int getSliceLength(int position)
    {
        return dictionary.getSliceLength(getId(position));
    }

    @Override
    public byte getByte(int position)
    {
        return dictionary.getByte(getId(position));
    }

    @Override
    public short getShort(int position)
    {
        return dictionary.getShort(getId(position));
    }

    @Override
    public int getInt(int position)
    {
        return dictionary.getInt(getId(position));
    }

    @Override
    public long getLong(int position)
    {
        return dictionary.getLong(getId(position));
    }

    @Override
    public long getLong(int position, int offset)
    {
        return dictionary.getLong(getId(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return dictionary.getSlice(getId(position), offset, length);
    }

    @Override
    public Block getBlock(int position)
    {
        return dictionary.getBlock(getId(position));
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return dictionary.bytesEqual(getId(position), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return dictionary.bytesCompare(getId(position), offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        dictionary.writeBytesTo(getId(position), offset, length, blockBuilder);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, SliceOutput sliceOutput)
    {
        dictionary.writeBytesTo(getId(position), offset, length, sliceOutput);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        dictionary.writePositionTo(getId(position), blockBuilder);
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        dictionary.writePositionTo(getId(position), output);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return dictionary.equals(getId(position), offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        return dictionary.hash(getId(position), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return dictionary.compareTo(getId(leftPosition), leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return dictionary.getSingleValueBlock(getId(position));
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes < 0) {
            calculateCompactSize();
        }
        return sizeInBytes;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        if (uniqueIds == positionCount) {
            // Each position is unique, so the per-position fixed size of the dictionary plus the dictionary id overhead
            // is our fixed size per position
            OptionalInt dictionarySizePerPosition = dictionary.fixedSizeInBytesPerPosition();
            if (dictionarySizePerPosition.isPresent()) {
                // Add overhead from the dictionary index
                return OptionalInt.of(dictionarySizePerPosition.getAsInt() + Integer.BYTES);
            }
        }
        return OptionalInt.empty();
    }

    private void calculateCompactSize()
    {
        int uniqueIds = 0;
        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = idsOffset; i < idsOffset + positionCount; i++) {
            int id = ids[i];
            uniqueIds += used[id] ? 0 : 1;
            used[id] = true;
        }
        this.uniqueIds = uniqueIds;
        this.sizeInBytes = getSizeInBytesForSelectedPositions(used, uniqueIds, positionCount);
    }

    @Override
    public long getLogicalSizeInBytes()
    {
        return getRegionLogicalSizeInBytes(0, getPositionCount());
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        if (positionOffset == 0 && length == getPositionCount()) {
            // Calculation of getRegionSizeInBytes is expensive in this class.
            // On the other hand, getSizeInBytes result is cached.
            return getSizeInBytes();
        }

        OptionalInt fixedSizePerPosition = fixedSizeInBytesPerPosition();
        if (fixedSizePerPosition.isPresent()) {
            return fixedSizePerPosition.getAsInt() * (long) length;
        }

        int uniqueIds = 0;
        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = idsOffset + positionOffset; i < idsOffset + positionOffset + length; i++) {
            int id = ids[i];
            uniqueIds += used[id] ? 0 : 1;
            used[id] = true;
        }
        return getSizeInBytesForSelectedPositions(used, uniqueIds, length);
    }

    @Override
    public long getRegionLogicalSizeInBytes(int positionOffset, int length)
    {
        if (positionOffset == 0 && length == getPositionCount() && logicalSizeInBytes >= 0) {
            return logicalSizeInBytes;
        }

        long sizeInBytes = 0;
        // Dictionary Block may contain large number of keys and small region length may be requested.
        // If the length is less than keys the cache is likely to be not used.
        if (length > dictionary.getPositionCount()) {
            // Cache code path.
            long[] seenSizes = new long[dictionary.getPositionCount()];
            Arrays.fill(seenSizes, -1L);
            for (int i = positionOffset; i < positionOffset + length; i++) {
                int position = getId(i);
                if (seenSizes[position] < 0) {
                    seenSizes[position] = dictionary.getRegionLogicalSizeInBytes(position, 1);
                }
                sizeInBytes += seenSizes[position];
            }
        }
        else {
            // In-place code path.
            for (int i = positionOffset; i < positionOffset + length; i++) {
                sizeInBytes += dictionary.getRegionLogicalSizeInBytes(getId(i), 1);
            }
        }

        if (positionOffset == 0 && length == getPositionCount()) {
            logicalSizeInBytes = sizeInBytes;
        }

        return sizeInBytes;
    }

    @Override
    public long getApproximateRegionLogicalSizeInBytes(int position, int length)
    {
        int dictionaryPositionCount = dictionary.getPositionCount();
        return dictionaryPositionCount == 0 ? 0 : dictionary.getApproximateRegionLogicalSizeInBytes(0, dictionaryPositionCount) * length / dictionaryPositionCount;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int usedPositionCount)
    {
        checkValidPositions(positions, positionCount);
        if (usedPositionCount == 0) {
            return 0;
        }
        if (usedPositionCount == positionCount) {
            return getSizeInBytes();
        }
        OptionalInt fixedSizePerPosition = fixedSizeInBytesPerPosition();
        if (fixedSizePerPosition.isPresent()) {
            return fixedSizePerPosition.getAsInt() * (long) usedPositionCount;
        }

        int uniqueIds = 0;
        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = 0; i < positions.length; i++) {
            int id = ids[idsOffset + i];
            if (positions[i]) {
                uniqueIds += used[id] ? 0 : 1;
                used[id] = true;
            }
        }
        return getSizeInBytesForSelectedPositions(used, uniqueIds, usedPositionCount);
    }

    private long getSizeInBytesForSelectedPositions(boolean[] usedIds, int uniqueIds, int selectedPositions)
    {
        long dictionarySize = dictionary.getPositionsSizeInBytes(usedIds, uniqueIds);
        if (uniqueIds == dictionary.getPositionCount() && this.uniqueIds == -1) {
            // All positions in the dictionary are referenced, store the uniqueId count and sizeInBytes
            this.uniqueIds = uniqueIds;
            this.sizeInBytes = dictionarySize + (Integer.BYTES * (long) positionCount);
        }
        return dictionarySize + (Integer.BYTES * (long) selectedPositions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return dictionary.getEstimatedDataSizeForStats(getId(position));
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(dictionary, dictionary.getRetainedSizeInBytes());
        consumer.accept(ids, sizeOf(ids));
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return DictionaryBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        IntArrayList positionsToCopy = new IntArrayList();
        Map<Integer, Integer> oldIndexToNewIndex = new HashMap<>();
        int[] newIds = new int[length];

        // Using a boxed integer to avoid repeated boxing.
        Integer nextIndex = 0;
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            int oldIndex = getId(position);
            Integer newIndex = oldIndexToNewIndex.putIfAbsent(oldIndex, nextIndex);
            if (newIndex == null) {
                newIndex = nextIndex;
                positionsToCopy.add(oldIndex);
                nextIndex = nextIndex + 1;
            }
            newIds[i] = newIndex;
        }
        return new DictionaryBlock(
                length,
                dictionary.copyPositions(positionsToCopy.elements(), 0, positionsToCopy.size()),
                newIds,
                true); // new dictionary is compact
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new DictionaryBlock(idsOffset + positionOffset, length, dictionary, ids, false, dictionarySourceId);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        checkValidRegion(positionCount, position, length);
        int[] newIds = Arrays.copyOfRange(ids, idsOffset + position, idsOffset + position + length);
        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, newIds);
        return dictionaryBlock.compact();
    }

    @Override
    public boolean isNull(int position)
    {
        return dictionary.isNull(getId(position));
    }

    @Override
    public boolean mayHaveNull()
    {
        return positionCount > 0 && dictionary.mayHaveNull();
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newIds = new int[length];
        boolean isCompact = length >= dictionary.getPositionCount() && isCompact();
        boolean[] usedIds = isCompact ? new boolean[dictionary.getPositionCount()] : null;
        int uniqueIds = 0;
        for (int i = 0; i < length; i++) {
            int id = getId(positions[offset + i]);
            newIds[i] = id;
            if (usedIds != null) {
                uniqueIds += usedIds[id] ? 0 : 1;
                usedIds[id] = true;
            }
        }
        // All positions must have been referenced in order to be compact
        isCompact &= (usedIds != null && usedIds.length == uniqueIds);
        DictionaryBlock result = new DictionaryBlock(newIds.length, dictionary, newIds, isCompact, dictionarySourceId);
        if (usedIds != null && !isCompact) {
            // resulting dictionary is not compact, but we know the number of unique ids and which positions are used
            result.uniqueIds = uniqueIds;
            result.sizeInBytes = dictionary.getPositionsSizeInBytes(usedIds, uniqueIds) + (Integer.BYTES * (long) length);
        }
        return result;
    }

    @Override
    public String toString()
    {
        return format("DictionaryBlock(%d){positionCount=%d,dictionary=%s}", hashCode(), getPositionCount(), dictionary.toString());
    }

    @Override
    public Block getLoadedBlock()
    {
        Block loadedDictionary = dictionary.getLoadedBlock();

        if (loadedDictionary == dictionary) {
            return this;
        }
        return new DictionaryBlock(idsOffset, getPositionCount(), loadedDictionary, ids, false, randomDictionaryId());
    }

    @Override
    public Block getUnderlyingValueBlock()
    {
        return dictionary.getUnderlyingValueBlock();
    }

    @Override
    public int getUnderlyingValuePosition(int position)
    {
        return dictionary.getUnderlyingValuePosition(getId(position));
    }

    public Block createProjection(Block newDictionary)
    {
        if (newDictionary.getPositionCount() != dictionary.getPositionCount()) {
            throw new IllegalArgumentException("newDictionary must have the same position count");
        }

        // if the new dictionary is lazy be careful to not materialize it
        if (newDictionary instanceof LazyBlock) {
            return new LazyBlock(positionCount, (block) -> {
                Block newDictionaryBlock = newDictionary.getBlock(0);
                Block newBlock = createProjection(newDictionaryBlock);
                block.setBlock(newBlock);
            });
        }
        if (newDictionary instanceof RunLengthEncodedBlock) {
            RunLengthEncodedBlock rle = (RunLengthEncodedBlock) newDictionary;
            return new RunLengthEncodedBlock(rle.getValue(), positionCount);
        }

        // unwrap dictionary in dictionary
        int[] newIds = new int[positionCount];
        for (int position = 0; position < positionCount; position++) {
            newIds[position] = newDictionary.getUnderlyingValuePosition(getIdUnchecked(position));
        }
        return new DictionaryBlock(0, positionCount, newDictionary.getUnderlyingValueBlock(), newIds, false, randomDictionaryId());
    }

    public Block getDictionary()
    {
        return dictionary;
    }

    Slice getIds()
    {
        return Slices.wrappedIntArray(ids, idsOffset, positionCount);
    }

    int[] getRawIds()
    {
        return ids;
    }

    /**
     * @param position the position of the desired value in this block
     * @return the position of the desired value in the underlying block this block wraps
     */
    public int getId(int position)
    {
        checkValidPosition(position, positionCount);
        return ids[position + idsOffset];
    }

    private int getIdUnchecked(int position)
    {
        return ids[position + idsOffset];
    }

    public DictionaryId getDictionarySourceId()
    {
        return dictionarySourceId;
    }

    public boolean isCompact()
    {
        if (dictionary.getPositionCount() > positionCount) {
            return false;
        }
        if (uniqueIds < 0) {
            calculateCompactSize();
        }
        return uniqueIds == dictionary.getPositionCount();
    }

    public DictionaryBlock compact()
    {
        if (isCompact()) {
            return this;
        }

        // determine which dictionary entries are referenced and build a reindex for them
        int dictionarySize = dictionary.getPositionCount();
        IntArrayList dictionaryPositionsToCopy = new IntArrayList(min(dictionarySize, positionCount));
        int[] remapIndex = new int[dictionarySize];
        Arrays.fill(remapIndex, -1);

        int newIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int dictionaryIndex = getId(i);
            if (remapIndex[dictionaryIndex] == -1) {
                dictionaryPositionsToCopy.add(dictionaryIndex);
                remapIndex[dictionaryIndex] = newIndex;
                newIndex++;
            }
        }

        // entire dictionary is referenced
        if (dictionaryPositionsToCopy.size() == dictionarySize) {
            return this;
        }

        // compact the dictionary
        int[] newIds = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            int newId = remapIndex[getId(i)];
            if (newId == -1) {
                throw new IllegalStateException("reference to a non-existent key");
            }
            newIds[i] = newId;
        }
        try {
            Block compactDictionary = dictionary.copyPositions(dictionaryPositionsToCopy.elements(), 0, dictionaryPositionsToCopy.size());
            return new DictionaryBlock(positionCount, compactDictionary, newIds, true);
        }
        catch (UnsupportedOperationException e) {
            // ignore if copy positions is not supported for the dictionary block
            return this;
        }
    }

    @Override
    public byte getByteUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return dictionary.getByte(ids[internalPosition]);
    }

    @Override
    public short getShortUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return dictionary.getShort(ids[internalPosition]);
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return dictionary.getInt(ids[internalPosition]);
    }

    @Override
    public long getLongUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return dictionary.getLong(ids[internalPosition]);
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return dictionary.getLong(ids[internalPosition], offset);
    }

    @Override
    public Slice getSliceUnchecked(int internalPosition, int offset, int length)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return dictionary.getSlice(ids[internalPosition], offset, length);
    }

    @Override
    public int getSliceLengthUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return dictionary.getSliceLength(ids[internalPosition]);
    }

    @Override
    public Block getBlockUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return dictionary.getBlock(ids[internalPosition]);
    }

    @Override
    public int getOffsetBase()
    {
        return idsOffset;
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, idsOffset, positionCount);
        return dictionary.isNull(ids[internalPosition]);
    }

    @Override
    public Block appendNull()
    {
        int desiredLength = idsOffset + positionCount + 1;
        int[] newIds = Arrays.copyOf(ids, desiredLength);

        Block newDictionary = dictionary;

        int nullIndex = NULL_NOT_FOUND;

        if (dictionary.mayHaveNull()) {
            int dictionaryPositionCount = dictionary.getPositionCount();
            for (int i = 0; i < dictionaryPositionCount; i++) {
                if (dictionary.isNull(i)) {
                    nullIndex = i;
                    break;
                }
            }
        }

        if (nullIndex == NULL_NOT_FOUND) {
            newIds[idsOffset + positionCount] = dictionary.getPositionCount();
            newDictionary = dictionary.appendNull();
        }
        else {
            newIds[idsOffset + positionCount] = nullIndex;
        }

        return new DictionaryBlock(idsOffset, positionCount + 1, newDictionary, newIds, isCompact(), getDictionarySourceId());
    }

    @Override
    public long toLong(int position)
    {
        return dictionary.toLong(getId(position));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DictionaryBlock other = (DictionaryBlock) obj;
        // Do not use mutable fields sizeInBytes, logicalSizeInBytes, uniqueIds as it makes the implementation non-deterministic.
        return this.positionCount == other.positionCount &&
                Objects.equals(this.dictionary, other.dictionary) &&
                this.idsOffset == other.idsOffset &&
                Arrays.equals(this.ids, other.ids) &&
                this.retainedSizeInBytes == other.retainedSizeInBytes &&
                Objects.equals(this.dictionarySourceId, other.dictionarySourceId);
    }

    @Override
    public int hashCode()
    {
        // Do not use mutable fields sizeInBytes, logicalSizeInBytes, uniqueIds as it makes the implementation non-deterministic.
        return Objects.hash(positionCount,
                dictionary,
                idsOffset,
                Arrays.hashCode(ids),
                retainedSizeInBytes,
                dictionarySourceId);
    }
}
