package com.facebook.presto.block;

import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TupleStreamSerdes
{
    // TODO: how can we associate this with the actual serdes?
    public static enum Encoding
    {
        RAW("raw"),
        RLE("rle"),
        DICTIONARY_RAW("dic-raw"),
        DICTIONARY_RLE("dic-rle");

        private static final Map<String, Encoding> NAME_MAP;
        static {
            ImmutableMap.Builder<String, Encoding> builder = ImmutableMap.builder();
            for (Encoding encoding : Encoding.values()) {
                builder.put(encoding.getName(), encoding);
            }
            NAME_MAP = builder.build();
        }

        private final String name;

        private Encoding(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }

        public static Encoding fromName(String name)
        {
            Encoding encoding = NAME_MAP.get(name);
            checkArgument(encoding != null, "Invalid type name: %s", name);
            return encoding;
        }
    }

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

    public static TupleStreamSerde createTupleStreamSerde(Encoding encoding)
    {
        switch (encoding) {
            case RAW:
                return new UncompressedSerde();
            case RLE:
                return new RunLengthEncodedSerde();
            case DICTIONARY_RAW:
                return new DictionarySerde(new UncompressedSerde());
            case DICTIONARY_RLE:
                return new DictionarySerde(new RunLengthEncodedSerde());
            default:
                throw new AssertionError("Missing encoding type!");
        }
    }
}
