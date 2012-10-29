package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.SizeOf;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.*;
import com.facebook.presto.block.dictionary.Dictionary.DictionaryBuilder;
import com.facebook.presto.block.uncompressed.UncompressedTupleInfoSerde;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.*;

public class DictionarySerde
        implements TupleStreamSerde
{
    private final TupleStreamSerde idSerde;

    @JsonCreator
    public DictionarySerde(@JsonProperty("idSerde") TupleStreamSerde idSerde)
    {
        this.idSerde = checkNotNull(idSerde, "idSerde is null");
    }

    @JsonProperty
    public TupleStreamSerde getIdSerde()
    {
        return idSerde;
    }

    @Override
    public TupleStreamSerializer createSerializer()
    {
        return new TupleStreamSerializer() {
            @Override
            public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
            {
                checkNotNull(sliceOutput, "sliceOutput is null");
                return new DictionaryTupleStreamWriter(idSerde.createSerializer().createTupleStreamWriter(sliceOutput), sliceOutput);
            }
        };
    }

    @Override
    public TupleStreamDeserializer createDeserializer()
    {
        return new TupleStreamDeserializer() {
            @Override
            public TupleStream deserialize(Range totalRange, Slice slice)
            {
                checkNotNull(slice, "slice is null");

                // Get dictionary byte length from tail and reset to beginning
                int dictionaryLength = slice.slice(slice.length() - SizeOf.SIZE_OF_INT, SizeOf.SIZE_OF_INT).input().readInt();

                // Slice out dictionary data and extract it
                Slice dictionarySlice = slice.slice(slice.length() - dictionaryLength - SizeOf.SIZE_OF_INT, dictionaryLength);
                Dictionary dictionary = deserializeDictionary(dictionarySlice);

                Slice payloadSlice = slice.slice(0, slice.length() - dictionaryLength - SizeOf.SIZE_OF_INT);

                return new DictionaryEncodedTupleStream(dictionary, idSerde.createDeserializer().deserialize(totalRange, payloadSlice));
            }

            @Override
            public Blocks deserializeBlocks(Range totalRange, Slice slice)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static int serializeDictionary(SliceOutput sliceOutput, Dictionary dictionary)
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

    private static Dictionary deserializeDictionary(Slice slice)
    {
        SliceInput sliceInput = slice.input();
        TupleInfo tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);

        int dictionarySize = sliceInput.readInt();
        checkArgument(dictionarySize >= 0);

        Slice[] dictionary = new Slice[dictionarySize];

        for (int i = 0; i < dictionarySize; i++) {
            dictionary[i] = tupleInfo.extractTupleSlice(sliceInput);
        }

        return new Dictionary(tupleInfo, dictionary);
    }

    private static class DictionaryTupleStreamWriter
            implements TupleStreamWriter
    {
        private final TupleStreamWriter idTupleStreamWriter;
        private final SliceOutput sliceOutput;
        private TupleInfo tupleInfo;
        private DictionaryBuilder dictionaryBuilder;
        private boolean finished;

        private DictionaryTupleStreamWriter(TupleStreamWriter idTupleStreamWriter, SliceOutput sliceOutput)
        {
            this.idTupleStreamWriter = checkNotNull(idTupleStreamWriter, "idTupleStreamWriter is null");
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
        }

        @Override
        public TupleStreamWriter append(final TupleStream tupleStream)
        {
            checkNotNull(tupleStream, "tupleStream is null");
            checkState(!finished, "already finished");

            if (tupleInfo == null) {
                tupleInfo = tupleStream.getTupleInfo();
                dictionaryBuilder = new DictionaryBuilder(tupleInfo);
            }

            TupleStream encodedTupleStream = new TupleStream()
            {
                @Override
                public TupleInfo getTupleInfo()
                {
                    return TupleInfo.SINGLE_LONG;
                }

                @Override
                public Range getRange()
                {
                    return tupleStream.getRange();
                }

                @Override
                public Cursor cursor(QuerySession session)
                {
                    Preconditions.checkNotNull(session, "session is null");
                    return new ForwardingCursor(tupleStream.cursor(session))
                    {
                        @Override
                        public TupleInfo getTupleInfo()
                        {
                            return TupleInfo.SINGLE_LONG;
                        }

                        @Override
                        public long getLong(int field)
                        {
                            return dictionaryBuilder.getId(getDelegate().getTuple());
                        }

                        @Override
                        public Tuple getTuple()
                        {
                            return TupleInfo.SINGLE_LONG.builder()
                                    .append(getLong(0))
                                    .build();
                        }

                        @Override
                        public Slice getSlice(int field)
                        {
                            return getTuple().getSlice(field);
                        }

                        @Override
                        public double getDouble(int field)
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public boolean currentTupleEquals(Tuple value)
                        {
                            return value.getTupleInfo().equals(TupleInfo.SINGLE_LONG) && value.getLong(0) == getLong(0);
                        }
                    };
                }
            };
            idTupleStreamWriter.append(encodedTupleStream);

            return this;
        }

        @Override
        public void finish()
        {
            checkState(tupleInfo != null, "nothing appended");
            checkState(!finished, "already finished");
            finished = true;

            idTupleStreamWriter.finish();

            // Serialize dictionary
            int dictionaryBytes = serializeDictionary(sliceOutput, dictionaryBuilder.build());

            // Write length of dictionary
            sliceOutput.writeInt(dictionaryBytes);
        }
    }
}
