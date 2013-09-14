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

import com.facebook.presto.block.dictionary.Dictionary;
import com.facebook.presto.block.uncompressed.UncompressedTupleInfoSerde;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkArgument;

public final class DictionarySerde
{
    private DictionarySerde()
    {
    }

    public static int writeDictionary(SliceOutput sliceOutput, Dictionary dictionary)
    {
        int bytesWritten = UncompressedTupleInfoSerde.serialize(dictionary.getTupleInfo(), sliceOutput);

        sliceOutput.writeInt(dictionary.size());
        bytesWritten += SizeOf.SIZE_OF_INT;
        for (int index = 0; index < dictionary.size(); index++) {
            Slice slice = dictionary.getTupleSlice(index);
            sliceOutput.writeBytes(slice);
            bytesWritten += slice.length();
        }
        return bytesWritten;
    }

    public static Dictionary readDictionary(Slice slice)
    {
        SliceInput sliceInput = slice.getInput();
        return readDictionary(sliceInput);
    }

    public static Dictionary readDictionary(SliceInput sliceInput)
    {
        TupleInfo tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);

        int dictionarySize = sliceInput.readInt();
        checkArgument(dictionarySize >= 0);

        Slice[] dictionary = new Slice[dictionarySize];

        for (int i = 0; i < dictionarySize; i++) {
            dictionary[i] = tupleInfo.extractTupleSlice(sliceInput);
        }

        return new Dictionary(tupleInfo, dictionary);
    }
}
