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
import com.facebook.presto.block.BlockEncoding;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.type.Type;
import com.facebook.presto.type.TypeManager;
import com.google.common.base.Preconditions;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<DictionaryBlockEncoding> FACTORY = new DictionaryBlockEncodingFactory();
    private static final String NAME = "DIC";

    private final RandomAccessBlock dictionary;
    private final BlockEncoding idBlockEncoding;

    public DictionaryBlockEncoding(RandomAccessBlock dictionary, BlockEncoding idBlockEncoding)
    {
        this.dictionary = checkNotNull(dictionary, "dictionary is null");
        this.idBlockEncoding = checkNotNull(idBlockEncoding, "idBlockEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Type getType()
    {
        return dictionary.getType();
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        DictionaryEncodedBlock dictionaryBlock = (DictionaryEncodedBlock) block;
        Preconditions.checkArgument(dictionaryBlock.getDictionary() == dictionary, "Block dictionary is not the same a this dictionary");
        idBlockEncoding.writeBlock(sliceOutput, dictionaryBlock.getIdBlock());
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        RandomAccessBlock idBlock = (RandomAccessBlock) idBlockEncoding.readBlock(sliceInput);
        return new DictionaryEncodedBlock(dictionary, idBlock);
    }

    private static class DictionaryBlockEncodingFactory
            implements BlockEncodingFactory<DictionaryBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public DictionaryBlockEncoding readEncoding(TypeManager typeManager, BlockEncodingManager blockEncodingManager, SliceInput input)
        {
            // read the dictionary
            BlockEncoding dictionaryEncoding = blockEncodingManager.readBlockEncoding(input);
            RandomAccessBlock dictionary = dictionaryEncoding.readBlock(input).toRandomAccessBlock();

            // read the id block encoding
            BlockEncoding idBlockEncoding = blockEncodingManager.readBlockEncoding(input);
            return new DictionaryBlockEncoding(dictionary, idBlockEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingManager blockEncodingManager, SliceOutput output, DictionaryBlockEncoding blockEncoding)
        {
            // write the dictionary
            BlockEncoding dictionaryBlockEncoding = blockEncoding.dictionary.getEncoding();
            blockEncodingManager.writeBlockEncoding(output, dictionaryBlockEncoding);
            dictionaryBlockEncoding.writeBlock(output, blockEncoding.dictionary);

            // write the id block encoding
            blockEncodingManager.writeBlockEncoding(output, blockEncoding.idBlockEncoding);
        }
    }
}
