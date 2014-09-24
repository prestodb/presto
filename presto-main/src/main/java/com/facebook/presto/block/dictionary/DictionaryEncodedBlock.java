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
package com.facebook.presto.block.dictionary;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.FixedWidthBlock;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.Slices.EMPTY_SLICE;

public class DictionaryEncodedBlock
        implements Block
{
    private static final FixedWidthBlock NULL_SINGLE_VALUE_BLOCK = new FixedWidthBlock(0, 1, EMPTY_SLICE, new boolean[] {true});
    private final Block dictionary;
    private final Block idBlock;

    public DictionaryEncodedBlock(Block dictionary, Block idBlock)
    {
        this.dictionary = checkNotNull(dictionary, "dictionary is null");
        this.idBlock = checkNotNull(idBlock, "idBlock is null");
    }

    public Block getDictionary()
    {
        return dictionary;
    }

    public Block getIdBlock()
    {
        return idBlock;
    }

    @Override
    public int getPositionCount()
    {
        return idBlock.getPositionCount();
    }

    @Override
    public int getSizeInBytes()
    {
        return Ints.checkedCast(dictionary.getSizeInBytes() + idBlock.getSizeInBytes());
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new DictionaryBlockEncoding(dictionary, idBlock.getEncoding());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return new DictionaryEncodedBlock(dictionary, idBlock.getRegion(positionOffset, length));
    }

    @Override
    public int getLength(int position)
    {
        return dictionary.getLength(getDictionaryKey(position));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return dictionary.getByte(getDictionaryKey(position), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return dictionary.getShort(getDictionaryKey(position), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return dictionary.getInt(getDictionaryKey(position), offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return dictionary.getLong(getDictionaryKey(position), offset);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        return dictionary.getFloat(getDictionaryKey(position), offset);
    }

    @Override
    public double getDouble(int position, int offset)
    {
        return dictionary.getDouble(getDictionaryKey(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return dictionary.getSlice(getDictionaryKey(position), offset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return dictionary.bytesEqual(getDictionaryKey(position), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return dictionary.bytesCompare(getDictionaryKey(position), offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        dictionary.writeBytesTo(getDictionaryKey(position), offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return dictionary.equals(getDictionaryKey(position), offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public int hash(int position, int offset, int length)
    {
        return dictionary.hash(getDictionaryKey(position), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return dictionary.compareTo(getDictionaryKey(leftPosition), leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        if (isNull(position)) {
            return NULL_SINGLE_VALUE_BLOCK;
        }
        return dictionary.getSingleValueBlock(getDictionaryKey(position));
    }

    @Override
    public boolean isNull(int position)
    {
        return idBlock.isNull(position) || dictionary.isNull(getDictionaryKey(position));
    }

    private int getDictionaryKey(int position)
    {
        return Ints.checkedCast(BIGINT.getLong(idBlock, position));
    }

    @Override
    public void assureLoaded()
    {
        dictionary.assureLoaded();
        idBlock.assureLoaded();
    }
}
