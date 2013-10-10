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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class DictionaryEncodedBlockCursor
        implements BlockCursor
{
    private final RandomAccessBlock dictionary;
    private final BlockCursor idCursor;

    public DictionaryEncodedBlockCursor(RandomAccessBlock dictionary, BlockCursor idCursor)
    {
        this.dictionary = checkNotNull(dictionary, "dictionary is null");
        this.idCursor = checkNotNull(idCursor, "idCursor is null");
        checkArgument(idCursor.getTupleInfo().equals(TupleInfo.SINGLE_LONG), "id cursor must contain tuples with a single long value");
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return dictionary.getTupleInfo();
    }

    @Override
    public int getRemainingPositions()
    {
        return idCursor.getRemainingPositions();
    }

    @Override
    public boolean isValid()
    {
        return idCursor.isValid();
    }

    @Override
    public boolean isFinished()
    {
        return idCursor.isFinished();
    }

    @Override
    public boolean advanceNextPosition()
    {
        return idCursor.advanceNextPosition();
    }

    @Override
    public boolean advanceToPosition(int position)
    {
        return idCursor.advanceToPosition(position);
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        return new DictionaryEncodedBlock(dictionary, (RandomAccessBlock) idCursor.getRegionAndAdvance(length));
    }

    @Override
    public Tuple getTuple()
    {
        return dictionary.getTuple(getDictionaryKey());
    }

    @Override
    public boolean getBoolean()
    {
        return dictionary.getBoolean(getDictionaryKey());
    }

    @Override
    public long getLong()
    {
        return dictionary.getLong(getDictionaryKey());
    }

    @Override
    public double getDouble()
    {
        return dictionary.getDouble(getDictionaryKey());
    }

    @Override
    public Slice getSlice()
    {
        return dictionary.getSlice(getDictionaryKey());
    }

    @Override
    public boolean isNull()
    {
        return dictionary.isNull(getDictionaryKey());
    }

    @Override
    public int getPosition()
    {
        return idCursor.getPosition();
    }

    @Override
    public int getRawOffset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getRawSlice()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTupleTo(BlockBuilder blockBuilder)
    {
        dictionary.appendTupleTo(getDictionaryKey(), blockBuilder);
    }

    public int getDictionaryKey()
    {
        int dictionaryKey = Ints.checkedCast(idCursor.getLong());
        checkPositionIndex(dictionaryKey, dictionary.getPositionCount(), "dictionaryKey does not exist");
        return dictionaryKey;
    }
}
