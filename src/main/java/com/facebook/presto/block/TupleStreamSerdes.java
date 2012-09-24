package com.facebook.presto.block;

import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Splitter;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TupleStreamSerdes
{
    public static void serialize(TupleStreamSerde serde, TupleStream tupleStream, SliceOutput sliceOutput)
    {
        checkNotNull(serde, "serde is null");
        checkNotNull(tupleStream, "tupleStream is null");
        checkNotNull(sliceOutput, "sliceOutput is null");
        serde.createTupleStreamWriter(sliceOutput).append(tupleStream).close();
    }
    
    public static TupleStream deserialize(TupleStreamSerde serde, Slice slice)
    {
        checkNotNull(serde, "serde is null");
        return serde.deserialize(slice);
    }

    public static TupleStreamSerde createTupleStreamSerde(String encoding)
    {
        // Example: 'rle', 'uncompressed', 'dic-rle', 'dic-uncompressed'
        Iterator<String> partsIterator = Splitter.on('-').limit(2).split(encoding).iterator();
        checkArgument(partsIterator.hasNext(), "encoding malformed: " + encoding);
        switch (partsIterator.next()) {
            case "raw":
                return new UncompressedSerde();
            case "rle":
                return new RunLengthEncodedSerde();
            case "dic":
                checkArgument(partsIterator.hasNext(), "dictionary encoding requires an embedded serde");
                return new DictionarySerde(createTupleStreamSerde(partsIterator.next()));
            default:
                throw new IllegalArgumentException("Unsupported encoding " + encoding);
        }
    }
}
