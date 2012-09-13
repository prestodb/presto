package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionarySerde implements BlockStreamSerde<DictionaryEncodedBlock>
{
    private final BlockStreamSerde<?> idSerde;

    public DictionarySerde(BlockStreamSerde<?> idSerde)
    {
        this.idSerde = idSerde;
    }

    @Override
    public void serialize(final BlockStream<? extends ValueBlock> blockStream, SliceOutput sliceOutput)
    {
        checkNotNull(blockStream, "blockStream is null");
        checkNotNull(sliceOutput, "sliceOutput is null");
        checkArgument(blockStream.getTupleInfo().getFieldCount() == 1, "Can only dictionary encode single columns");

        final DictionaryBuilder dictionaryBuilder = new DictionaryBuilder();
        BlockStream<ValueBlock> encodedBlockStream = new BlockStream<ValueBlock>()
        {
            @Override
            public TupleInfo getTupleInfo()
            {
                return blockStream.getTupleInfo();
            }

            @Override
            public Cursor cursor()
            {
                return new ForwardingCursor(blockStream.cursor())
                {
                    @Override
                    public TupleInfo getTupleInfo()
                    {
                        return TupleInfo.SINGLE_LONG_TUPLEINFO;
                    }

                    @Override
                    public long getLong(int field)
                    {
                        return dictionaryBuilder.getId(getDelegate().getTuple());
                    }

                    @Override
                    public Tuple getTuple()
                    {
                        return TupleInfo.SINGLE_LONG_TUPLEINFO.builder()
                                .append(getLong(0))
                                .build();
                    }

                    @Override
                    public Slice getSlice(int field)
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public Iterator<ValueBlock> iterator()
            {
                throw new UnsupportedOperationException();
            }
        };
        idSerde.serialize(encodedBlockStream, sliceOutput);

        // Serialize Footer
        int footerBytes = new Footer(blockStream.getTupleInfo(), dictionaryBuilder.build()).serialize(sliceOutput);

        // Write length of Footer
        sliceOutput.writeInt(footerBytes);
    }

    @Override
    public BlockStream<DictionaryEncodedBlock> deserialize(Slice slice)
    {
        checkNotNull(slice, "slice is null");

        // Get Footer byte length from tail and reset to beginning
        int footerLen = slice.slice(slice.length() - SizeOf.SIZE_OF_INT, SizeOf.SIZE_OF_INT).input().readInt();

        // Slice out Footer data and extract it
        Slice footerSlice = slice.slice(slice.length() - footerLen - SizeOf.SIZE_OF_INT, footerLen);
        Footer footer = Footer.deserialize(footerSlice);

        Slice payloadSlice = slice.slice(0, slice.length() - footerLen - SizeOf.SIZE_OF_INT);

        return new DictionaryEncodedBlockStream(footer.getTupleInfo(), footer.getDictionary(), idSerde.deserialize(payloadSlice));
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
            int startBytesWritable = sliceOutput.writableBytes();

            UncompressedTupleInfoSerde.serialize(tupleInfo, sliceOutput);

            sliceOutput.writeInt(dictionary.length);
            for (Slice slice : dictionary) {
                // Write length
                sliceOutput.writeInt(slice.length());
                // Write value
                sliceOutput.writeBytes(slice);
            }

            int endBytesWritable = sliceOutput.writableBytes();
            return startBytesWritable - endBytesWritable;
        }

        private static Footer deserialize(Slice slice)
        {
            SliceInput sliceInput = slice.input();
            TupleInfo tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);

            int dictionarySize = sliceInput.readInt();
            checkArgument(dictionarySize >= 0);

            Slice[] dictionary = new Slice[dictionarySize];

            for (int i = 0; i < dictionarySize; i++) {
                // Read value Length
                int sliceLength = sliceInput.readInt();
                checkArgument(sliceLength >= 0);
                // Read and store value
                dictionary[i] = sliceInput.readSlice(sliceLength);
            }

            return new Footer(tupleInfo, dictionary);
        }
    }
}
