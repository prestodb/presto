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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedBlock
        implements RandomAccessBlock
{
    private final RandomAccessBlock dictionary;
    private final RandomAccessBlock idBlock;

    public DictionaryEncodedBlock(RandomAccessBlock dictionary, RandomAccessBlock idBlock)
    {
        this.dictionary = checkNotNull(dictionary, "dictionary is null");
        this.idBlock = checkNotNull(idBlock, "idBlock is null");
        checkArgument(idBlock.getType().equals(BIGINT), "Expected bigint block but got %s block", idBlock.getType());
    }

    @Override
    public Type getType()
    {
        return dictionary.getType();
    }

    public RandomAccessBlock getDictionary()
    {
        return dictionary;
    }

    public RandomAccessBlock getIdBlock()
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
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        return new DictionaryEncodedBlock(dictionary, idBlock.getRegion(positionOffset, length));
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return this;
    }

    @Override
    public DictionaryEncodedBlockCursor cursor()
    {
        return new DictionaryEncodedBlockCursor(dictionary, idBlock.cursor());
    }

    @Override
    public boolean getBoolean(int position)
    {
        return dictionary.getBoolean(getDictionaryKey(position));
    }

    @Override
    public long getLong(int position)
    {
        return dictionary.getLong(getDictionaryKey(position));
    }

    @Override
    public double getDouble(int position)
    {
        return dictionary.getDouble(getDictionaryKey(position));
    }

    @Override
    public Slice getSlice(int position)
    {
        return dictionary.getSlice(getDictionaryKey(position));
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        return dictionary.getSingleValueBlock(getDictionaryKey(position));
    }

    @Override
    public Object getObjectValue(int position)
    {
        return dictionary.getObjectValue(getDictionaryKey(position));
    }

    @Override
    public boolean isNull(int position)
    {
        return dictionary.isNull(getDictionaryKey(position));
    }

    @Override
    public boolean equals(int position, RandomAccessBlock right, int rightPosition)
    {
        return dictionary.equals(getDictionaryKey(position), right, rightPosition);
    }

    @Override
    public boolean equals(int position, BlockCursor value)
    {
        return dictionary.equals(getDictionaryKey(position), value);
    }

    @Override
    public boolean equals(int position, Slice slice, int offset)
    {
        return dictionary.equals(getDictionaryKey(position), slice, offset);
    }

    @Override
    public int hashCode(int position)
    {
        return dictionary.hashCode(getDictionaryKey(position));
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock right, int rightPosition)
    {
        return dictionary.compareTo(sortOrder, getDictionaryKey(position), right, rightPosition);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        return dictionary.compareTo(sortOrder, getDictionaryKey(position), cursor);
    }

    @Override
    public int compareTo(int position, Slice slice, int offset)
    {
        return dictionary.compareTo(getDictionaryKey(position), slice, offset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        dictionary.appendTo(getDictionaryKey(position), blockBuilder);
    }

    private int getDictionaryKey(int position)
    {
        return Ints.checkedCast(idBlock.getLong(position));
    }
}
