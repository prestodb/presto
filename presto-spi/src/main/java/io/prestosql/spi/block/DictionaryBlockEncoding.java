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
package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

public class DictionaryBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "DICTIONARY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        DictionaryBlock dictionaryBlock = (DictionaryBlock) block;

        dictionaryBlock = dictionaryBlock.compact();

        // positionCount
        int positionCount = dictionaryBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // dictionary
        Block dictionary = dictionaryBlock.getDictionary();
        blockEncodingSerde.writeBlock(sliceOutput, dictionary);

        // ids
        sliceOutput.writeBytes(dictionaryBlock.getIds());

        // instance id
        sliceOutput.appendLong(dictionaryBlock.getDictionarySourceId().getMostSignificantBits());
        sliceOutput.appendLong(dictionaryBlock.getDictionarySourceId().getLeastSignificantBits());
        sliceOutput.appendLong(dictionaryBlock.getDictionarySourceId().getSequenceId());
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        // positionCount
        int positionCount = sliceInput.readInt();

        // dictionary
        Block dictionaryBlock = blockEncodingSerde.readBlock(sliceInput);

        // ids
        int[] ids = new int[positionCount];
        sliceInput.readBytes(Slices.wrappedIntArray(ids));

        // instance id
        long mostSignificantBits = sliceInput.readLong();
        long leastSignificantBits = sliceInput.readLong();
        long sequenceId = sliceInput.readLong();

        // We always compact the dictionary before we send it. However, dictionaryBlock comes from sliceInput, which may over-retain memory.
        // As a result, setting dictionaryIsCompacted to true is not appropriate here.
        // TODO: fix DictionaryBlock so that dictionaryIsCompacted can be set to true when the underlying block over-retains memory.
        return new DictionaryBlock(positionCount, dictionaryBlock, ids, false, new DictionaryId(mostSignificantBits, leastSignificantBits, sequenceId));
    }
}
