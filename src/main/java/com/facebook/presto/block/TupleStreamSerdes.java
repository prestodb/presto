package com.facebook.presto.block;

import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.block.StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TupleStreamSerdes
{
    public static void serialize(TupleStreamSerde serde, TupleStream tupleStream, SliceOutput sliceOutput)
    {
        checkNotNull(serde, "serde is null");
        checkNotNull(tupleStream, "tupleStream is null");
        checkNotNull(sliceOutput, "sliceOutput is null");
        serde.createSerializer().createTupleStreamWriter(sliceOutput).append(tupleStream).finish();
    }

    public static TupleStream deserialize(TupleStreamSerde serde, Slice slice)
    {
        checkNotNull(serde, "serde is null");
        return serde.createDeserializer().deserialize(slice);
    }

    /**
     * Should only be used when paired with createDefaultDeserializer(...)
     */
    public static DefaultTupleStreamSerializer createDefaultSerializer(TupleStreamSerde coreSerde)
    {
        return new DefaultTupleStreamSerializer(coreSerde);
    }

    /**
     * Should only be used when paired with createDefaultSerializer(...)
     */
    public static DefaultTupleStreamDeserializer createDefaultDeserializer()
    {
        return DefaultTupleStreamDeserializer.INSTANCE;
    }

    public static class DefaultTupleStreamSerializer
            implements TupleStreamSerializer
    {
        private final TupleStreamSerializer defaultSerializer;

        public DefaultTupleStreamSerializer(TupleStreamSerde coreSerde)
        {
            defaultSerializer = new StatsCollectingTupleStreamSerde(new SelfDescriptiveSerde(checkNotNull(coreSerde, "coreSerde is null"))).createSerializer();
        }

        @Override
        public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
        {
            return defaultSerializer.createTupleStreamWriter(checkNotNull(sliceOutput, "sliceOutput is null"));
        }

    }

    public static class DefaultTupleStreamDeserializer
            implements TupleStreamDeserializer
    {
        private static final DefaultTupleStreamDeserializer INSTANCE = new DefaultTupleStreamDeserializer();

        private static final StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStreamDeserializer DESERIALIZER = new StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStreamDeserializer(SelfDescriptiveSerde.DESERIALIZER);

        @Override
        public StatsAnnotatedTupleStream deserialize(Slice slice)
        {
            return DESERIALIZER.deserialize(slice);
        }
    }

    public static enum Encoding
    {
        RAW("raw")
                {
                    @Override
                    public TupleStreamSerde createSerde()
                    {
                        return new UncompressedSerde();
                    }
                },
        RLE("rle")
                {
                    @Override
                    public TupleStreamSerde createSerde()
                    {
                        return new RunLengthEncodedSerde();
                    }
                },
        DICTIONARY_RAW("dicraw")
                {
                    @Override
                    public TupleStreamSerde createSerde()
                    {
                        return new DictionarySerde(RAW.createSerde());
                    }
                },
        DICTIONARY_RLE("dicrle")
                {
                    @Override
                    public TupleStreamSerde createSerde()
                    {
                        return new DictionarySerde(RLE.createSerde());
                    }
                };

        private static final Map<String, Encoding> NAME_MAP;
        static {
            ImmutableMap.Builder<String, Encoding> builder = ImmutableMap.builder();
            for (Encoding encoding : Encoding.values()) {
                builder.put(encoding.getName(), encoding);
            }
            NAME_MAP = builder.build();
        }

        // Name should be usable as a filename
        private final String name;

        private Encoding(String name)
        {
            this.name = checkNotNull(name, "name is null");
        }

        public String getName()
        {
            return name;
        }

        public abstract TupleStreamSerde createSerde();

        public static Encoding fromName(String name)
        {
            checkNotNull(name, "name is null");
            Encoding encoding = NAME_MAP.get(name);
            checkArgument(encoding != null, "Invalid type name: %s", name);
            return encoding;
        }
    }
}
