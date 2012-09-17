package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.SizeOf;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.*;
import com.facebook.presto.block.uncompressed.UncompressedTupleInfoSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.*;

public class DictionarySerde
        implements TupleStreamSerde
{
    private final TupleStreamSerde idSerde;

    public DictionarySerde(TupleStreamSerde idSerde)
    {
        this.idSerde = checkNotNull(idSerde, "idSerde is null");
    }

    @Override
    public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
    {
        checkNotNull(sliceOutput, "sliceOutput is null");
        return new DictionaryTupleStreamWriter(idSerde.createTupleStreamWriter(sliceOutput), sliceOutput);
    }

    @Override
    public TupleStream deserialize(Slice slice)
    {
        checkNotNull(slice, "slice is null");

        // Get Footer byte length from tail and reset to beginning
        int footerLen = slice.slice(slice.length() - SizeOf.SIZE_OF_INT, SizeOf.SIZE_OF_INT).input().readInt();

        // Slice out Footer data and extract it
        Slice footerSlice = slice.slice(slice.length() - footerLen - SizeOf.SIZE_OF_INT, footerLen);
        Footer footer = Footer.deserialize(footerSlice);

        Slice payloadSlice = slice.slice(0, slice.length() - footerLen - SizeOf.SIZE_OF_INT);

        return new DictionaryEncodedTupleStream(footer.getTupleInfo(), footer.getDictionary(), idSerde.deserialize(payloadSlice));
    }

    private static class DictionaryBuilder
    {
        private final Map<Slice, Integer> dictionary = new HashMap<>();
        private int nextId = 0;

        public long getId(Tuple tuple)
        {
            Integer id = dictionary.get(tuple.getTupleSlice());
            if (id == null) {
                id = nextId;
                nextId++;
                dictionary.put(tuple.getTupleSlice(), id);
            }
            return id;
        }

        public Slice[] build()
        {
            // Convert ID map to compact dictionary array (should be contiguous)
            Slice[] dictionary = new Slice[this.dictionary.size()];
            for (Entry<Slice, Integer> entry : this.dictionary.entrySet()) {
                dictionary[entry.getValue()] = entry.getKey();
            }
            return dictionary;
        }
    }

    // TODO: this encoding can be made more compact if we leverage sorted order of the map
    private static class Footer
    {
        private final TupleInfo tupleInfo;
        private final Slice[] dictionary;

        private Footer(TupleInfo tupleInfo, Slice[] dictionary)
        {
            this.tupleInfo = tupleInfo;
            this.dictionary = dictionary;
        }

        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        public Slice[] getDictionary()
        {
            return dictionary;
        }

        /**
         * @return bytes written to sliceOutput
         */
        private int serialize(SliceOutput sliceOutput)
        {
            int bytesWritten = UncompressedTupleInfoSerde.serialize(tupleInfo, sliceOutput);

            sliceOutput.writeInt(dictionary.length);
            bytesWritten += SizeOf.SIZE_OF_INT;
            for (Slice slice : dictionary) {
                sliceOutput.writeBytes(slice);
                bytesWritten += slice.length();
            }
            return bytesWritten;
        }

        private static Footer deserialize(Slice slice)
        {
            SliceInput sliceInput = slice.input();
            TupleInfo tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);

            int dictionarySize = sliceInput.readInt();
            checkArgument(dictionarySize >= 0);

            Slice[] dictionary = new Slice[dictionarySize];

            for (int i = 0; i < dictionarySize; i++) {
                dictionary[i] = tupleInfo.extractTupleSlice(sliceInput);
            }

            return new Footer(tupleInfo, dictionary);
        }
    }

    private static class DictionaryTupleStreamWriter
            implements TupleStreamWriter
    {
        private final TupleStreamWriter idTupleStreamWriter;
        private final SliceOutput sliceOutput;
        private final DictionaryBuilder dictionaryBuilder = new DictionaryBuilder();
        private TupleInfo tupleInfo;
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
                public Cursor cursor()
                {
                    return new ForwardingCursor(tupleStream.cursor())
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
        public void close()
        {
            checkState(tupleInfo != null, "nothing appended");
            checkState(!finished, "already finished");
            finished = true;

            idTupleStreamWriter.close();

            // Serialize Footer
            int footerBytes = new Footer(tupleInfo, dictionaryBuilder.build()).serialize(sliceOutput);

            // Write length of Footer
            sliceOutput.writeInt(footerBytes);
        }
    }
}
