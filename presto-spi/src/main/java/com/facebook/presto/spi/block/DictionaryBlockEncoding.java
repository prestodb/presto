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

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static java.util.Objects.requireNonNull;

public class DictionaryBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<DictionaryBlockEncoding> FACTORY = new DictionaryBlockEncodingFactory();
    private static final String NAME = "DICTIONARY";
    private final BlockEncoding dictionaryEncoding;

    public DictionaryBlockEncoding(BlockEncoding dictionaryEncoding)
    {
        this.dictionaryEncoding = requireNonNull(dictionaryEncoding, "dictionaryEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        DictionaryBlock dictionaryBlock = (DictionaryBlock) block;

        dictionaryBlock = dictionaryBlock.compact();

        // positionCount
        int positionCount = dictionaryBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // dictionary
        Block dictionary = dictionaryBlock.getDictionary();
        dictionaryEncoding.writeBlock(sliceOutput, dictionary);

        // ids
        Slice ids = dictionaryBlock.getIds();
        sliceOutput
                .appendInt(ids.length())
                .writeBytes(ids);

        // instance id
        sliceOutput.appendLong(dictionaryBlock.getDictionarySourceId().getMostSignificantBits());
        sliceOutput.appendLong(dictionaryBlock.getDictionarySourceId().getLeastSignificantBits());
        sliceOutput.appendLong(dictionaryBlock.getDictionarySourceId().getSequenceId());
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        // positionCount
        int positionCount = sliceInput.readInt();

        // dictionary
        Block dictionaryBlock = dictionaryEncoding.readBlock(sliceInput);

        // ids
        int lengthIdsSlice = sliceInput.readInt();
        Slice ids = sliceInput.readSlice(lengthIdsSlice);

        // instance id
        long mostSignificantBits = sliceInput.readLong();
        long leastSignificantBits = sliceInput.readLong();
        long sequenceId = sliceInput.readLong();

        // we always compact the dictionary before we send it
        return new DictionaryBlock(positionCount, dictionaryBlock, ids, true, new DictionaryId(mostSignificantBits, leastSignificantBits, sequenceId));
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public BlockEncoding getDictionaryEncoding()
    {
        return dictionaryEncoding;
    }

    public static class DictionaryBlockEncodingFactory
            implements BlockEncodingFactory<DictionaryBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public DictionaryBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            BlockEncoding dictionaryEncoding = serde.readBlockEncoding(input);
            return new DictionaryBlockEncoding(dictionaryEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, DictionaryBlockEncoding blockEncoding)
        {
            serde.writeBlockEncoding(output, blockEncoding.getDictionaryEncoding());
        }
    }
}
