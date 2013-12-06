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
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.serde.DictionaryBlockEncoding;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedBlock
        implements Block
{
    private final Dictionary dictionary;
    private final Block idBlock;

    public DictionaryEncodedBlock(Dictionary dictionary, Block idBlock)
    {
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(idBlock, "block is null");
        checkArgument(idBlock.getTupleInfo().equals(TupleInfo.SINGLE_LONG), "block must contain tuples with a single long value");

        this.dictionary = dictionary;
        this.idBlock = idBlock;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return dictionary.getTupleInfo();
    }

    public Dictionary getDictionary()
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
    public DataSize getDataSize()
    {
        // todo include dictionary size
        return idBlock.getDataSize();
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
    public RandomAccessBlock toRandomAccessBlock()
    {
        // todo add a RandomAccessDictionaryEncodedBlock that contains a RandomAccessBlock for the ids
        throw new UnsupportedOperationException();
    }

    @Override
    public DictionaryEncodedBlockCursor cursor()
    {
        return new DictionaryEncodedBlockCursor(dictionary, idBlock.cursor());
    }
}
