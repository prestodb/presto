package com.facebook.presto.block;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;

import static com.facebook.presto.block.StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStream;
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
            defaultSerializer = new StatsCollectingTupleStreamSerde(new SelfIdTupleStreamSerde(checkNotNull(coreSerde, "coreSerde is null"))).createSerializer();
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

        private static final StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStreamDeserializer DESERIALIZER = new StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStreamDeserializer(SelfIdTupleStreamSerde.DESERIALIZER);

        @Override
        public StatsAnnotatedTupleStream deserialize(Slice slice)
        {
            return DESERIALIZER.deserialize(slice);
        }
    }
}
