/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.SizeOf;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.dictionary.Dictionary;
import com.facebook.presto.block.uncompressed.UncompressedTupleInfoSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;

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
