package com.facebook.presto.block;

import com.facebook.presto.SizeOf;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Stage;
import io.airlift.json.JsonModule;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SelfIdTupleStreamSerde
        implements TupleStreamSerde
{
    private static final ObjectMapper OBJECT_MAPPER = Guice.createInjector(Stage.PRODUCTION, new JsonModule()).getInstance(ObjectMapper.class);
    public static final TupleStreamDeserializer DESERIALIZER = new SelfIdTupleStreamDeserializer();

    private final TupleStreamSerde serializationSerde;

    /**
     * Provided tupleStreamSerde must be registered with TupleStreamSerde interface
     */
    public SelfIdTupleStreamSerde(TupleStreamSerde serializationSerde)
    {
        this.serializationSerde = checkNotNull(serializationSerde, "serializationSerde is null");
    }

    @Override
    public TupleStreamSerializer createSerializer()
    {
        return new TupleStreamSerializer() {
            @Override
            public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
            {
                checkNotNull(sliceOutput, "sliceOutput is null");
                try {
                    // TODO: we can make this more efficient by writing the length in the footer
                    byte[] header = OBJECT_MAPPER.writeValueAsBytes(serializationSerde);
                    sliceOutput.writeInt(header.length);
                    sliceOutput.writeBytes(header);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                return serializationSerde.createSerializer().createTupleStreamWriter(sliceOutput);
            }
        };
    }

    @Override
    public TupleStreamDeserializer createDeserializer()
    {
        // Only one instance necessary
        return DESERIALIZER;
    }

    private static class SelfIdTupleStreamDeserializer
            implements TupleStreamDeserializer
    {
        @Override
        public TupleStream deserialize(Slice slice)
        {
            checkNotNull(slice, "slice is null");
            int headerLength = slice.getInt(0);
            int headerOffset = SizeOf.SIZE_OF_INT;
            int dataOffset = headerOffset + headerLength;
            try {
                TupleStreamSerde tupleStreamSerde = OBJECT_MAPPER.readValue(slice.slice(headerOffset, headerLength).input(), TupleStreamSerde.class);
                return tupleStreamSerde.createDeserializer().deserialize(slice.slice(dataOffset, slice.length() - dataOffset));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
