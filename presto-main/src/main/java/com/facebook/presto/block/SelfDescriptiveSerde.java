package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.SizeOf;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Throwables;
import io.airlift.json.ObjectMapperProvider;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SelfDescriptiveSerde
        implements TupleStreamSerde
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    public static final TupleStreamDeserializer DESERIALIZER = new SelfIdTupleStreamDeserializer();

    private final TupleStreamSerde tupleStreamSerde;

    /**
     * Provided tupleStreamSerde must be registered with TupleStreamSerde interface
     */
    public SelfDescriptiveSerde(TupleStreamSerde tupleStreamSerde)
    {
        this.tupleStreamSerde = checkNotNull(tupleStreamSerde, "tupleStreamSerde is null");
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
                    byte[] header = OBJECT_MAPPER.writeValueAsBytes(tupleStreamSerde);
                    sliceOutput.writeInt(header.length);
                    sliceOutput.writeBytes(header);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                return tupleStreamSerde.createSerializer().createTupleStreamWriter(sliceOutput);
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
        public TupleStream deserialize(Range totalRange, Slice slice)
        {
            checkNotNull(slice, "slice is null");
            int headerLength = slice.getInt(0);
            int headerOffset = SizeOf.SIZE_OF_INT;
            int dataOffset = headerOffset + headerLength;
            try {
                TupleStreamSerde tupleStreamSerde = OBJECT_MAPPER.readValue(slice.slice(headerOffset, headerLength).input(), TupleStreamSerde.class);
                return tupleStreamSerde.createDeserializer().deserialize(totalRange, slice.slice(dataOffset, slice.length() - dataOffset));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
