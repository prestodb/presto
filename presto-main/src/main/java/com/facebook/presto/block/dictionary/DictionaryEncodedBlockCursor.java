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
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class DictionaryEncodedBlockCursor
        implements BlockCursor
{
    private final Dictionary dictionary;
    private final BlockCursor sourceCursor;

    public DictionaryEncodedBlockCursor(Dictionary dictionary, BlockCursor sourceCursor)
    {
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceCursor, "sourceCursor is null");

        this.dictionary = dictionary;
        this.sourceCursor = sourceCursor;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return dictionary.getTupleInfo();
    }

    @Override
    public int getRemainingPositions()
    {
        return sourceCursor.getRemainingPositions();
    }

    @Override
    public boolean isValid()
    {
        return sourceCursor.isValid();
    }

    @Override
    public boolean isFinished()
    {
        return sourceCursor.isFinished();
    }

    @Override
    public boolean advanceNextPosition()
    {
        return sourceCursor.advanceNextPosition();
    }

    @Override
    public boolean advanceToPosition(int position)
    {
        return sourceCursor.advanceToPosition(position);
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        return new DictionaryEncodedBlock(dictionary, sourceCursor.getRegionAndAdvance(length));
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
        return sourceCursor.getPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        return dictionary.tupleEquals(getDictionaryKey(), value);
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
        int dictionaryKey = Ints.checkedCast(sourceCursor.getLong());
        checkPositionIndex(dictionaryKey, dictionary.size(), "dictionaryKey does not exist");
        return dictionaryKey;
    }
}
