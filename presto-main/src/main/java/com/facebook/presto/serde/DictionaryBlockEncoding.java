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
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.dictionary.Dictionary;
import com.facebook.presto.block.dictionary.DictionaryEncodedBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryBlockEncoding
        implements BlockEncoding
{
    private final Dictionary dictionary;
    private final BlockEncoding idBlockEncoding;

    public DictionaryBlockEncoding(Dictionary dictionary, BlockEncoding idBlockEncoding)
    {
        this.dictionary = checkNotNull(dictionary, "dictionary is null");
        this.idBlockEncoding = checkNotNull(idBlockEncoding, "idBlockEncoding is null");
    }

    public DictionaryBlockEncoding(SliceInput input)
    {
        dictionary = DictionarySerde.readDictionary(input);
        idBlockEncoding = BlockEncodings.readBlockEncoding(input);
    }

    public Dictionary getDictionary()
    {
        return dictionary;
    }

    public BlockEncoding getIdBlockEncoding()
    {
        return idBlockEncoding;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return dictionary.getTupleInfo();
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
        Block idBlock = idBlockEncoding.readBlock(sliceInput);
        return new DictionaryEncodedBlock(dictionary, idBlock);
    }

    public static void serialize(SliceOutput output, DictionaryBlockEncoding encoding)
    {
        DictionarySerde.writeDictionary(output, encoding.dictionary);
        BlockEncodings.writeBlockEncoding(output, encoding.idBlockEncoding);
    }
}
