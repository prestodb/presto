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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.spi.block.BlockUtil.checkValidPosition;
import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.spi.block.DictionaryId.randomDictionaryId;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class DictionaryBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryBlock.class).instanceSize() + ClassLayout.parseClass(DictionaryId.class).instanceSize();

    private final int positionCount;
    private final Block dictionary;
    private final int idsOffset;
    private final int[] ids;
    private final long retainedSizeInBytes;
    private volatile long sizeInBytes = -1;
    private volatile int uniqueIds = -1;
    private final DictionaryId dictionarySourceId;

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
            this.sizeInBytes = this.retainedSizeInBytes;
            this.uniqueIds = dictionary.getPositionCount();
        }
    }

    @Override
    public int getSliceLength(int position)
    {
        return dictionary.getSliceLength(getId(position));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return dictionary.getByte(getId(position), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return dictionary.getShort(getId(position), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return dictionary.getInt(getId(position), offset);
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
    public <T> T getObject(int position, Class<T> clazz)
    {
        return dictionary.getObject(getId(position), clazz);
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
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        dictionary.writePositionTo(getId(position), blockBuilder);
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

    private void calculateCompactSize()
    {
        long sizeInBytes = 0;
        int uniqueIds = 0;
        boolean[] seen = new boolean[dictionary.getPositionCount()];
        for (int i = 0; i < positionCount; i++) {
            int position = getId(i);
            if (!seen[position]) {
                if (!dictionary.isNull(position)) {
                    sizeInBytes += dictionary.getRegionSizeInBytes(position, 1);
                }
                uniqueIds++;
                seen[position] = true;
            }
        }
        this.sizeInBytes = sizeInBytes + (Integer.BYTES * (long) positionCount);
        this.uniqueIds = uniqueIds;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        if (positionOffset == 0 && length == getPositionCount()) {
            // Calculation of getRegionSizeInBytes is expensive in this class.
            // On the other hand, getSizeInBytes result is cached.
            return getSizeInBytes();
        }

        long sizeInBytes = 0;
        boolean[] seen = new boolean[dictionary.getPositionCount()];
        for (int i = positionOffset; i < positionOffset + length; i++) {
            int position = getId(i);
            if (!seen[position]) {
                if (!dictionary.isNull(position)) {
                    sizeInBytes += dictionary.getRegionSizeInBytes(position, 1);
                }
                seen[position] = true;
            }
        }
        sizeInBytes += Integer.BYTES * (long) length;
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(dictionary, dictionary.getRetainedSizeInBytes());
        consumer.accept(ids, sizeOf(ids));
        consumer.accept(this, (long) INSTANCE_SIZE);
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

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            int oldIndex = getId(position);
            if (!oldIndexToNewIndex.containsKey(oldIndex)) {
                oldIndexToNewIndex.put(oldIndex, positionsToCopy.size());
                positionsToCopy.add(oldIndex);
            }
            newIds[i] = oldIndexToNewIndex.get(oldIndex);
        }
        return new DictionaryBlock(dictionary.copyPositions(positionsToCopy.elements(), 0, positionsToCopy.size()), newIds);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new DictionaryBlock(idsOffset + positionOffset, length, dictionary, ids, false, randomDictionaryId());
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
    public Block getPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newIds = new int[length];
        boolean isCompact = isCompact() && length >= dictionary.getPositionCount();
        boolean[] seen = null;
        if (isCompact) {
            seen = new boolean[dictionary.getPositionCount()];
        }
        for (int i = 0; i < length; i++) {
            newIds[i] = getId(positions[offset + i]);
            if (isCompact) {
                seen[newIds[i]] = true;
            }
        }
        for (int i = 0; i < dictionary.getPositionCount() && isCompact; i++) {
            isCompact &= seen[i];
        }
        return new DictionaryBlock(newIds.length, getDictionary(), newIds, isCompact, getDictionarySourceId());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("DictionaryBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
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

    public Block getDictionary()
    {
        return dictionary;
    }

    Slice getIds()
    {
        return Slices.wrappedIntArray(ids, idsOffset, positionCount);
    }

    public int getId(int position)
    {
        checkValidPosition(position, positionCount);
        return ids[position + idsOffset];
    }

    public DictionaryId getDictionarySourceId()
    {
        return dictionarySourceId;
    }

    public boolean isCompact()
    {
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
}
