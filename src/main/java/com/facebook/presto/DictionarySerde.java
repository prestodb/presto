package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionarySerde implements BlockStreamSerde<DictionaryEncodedBlock>
{
    private final BlockStreamSerde idSerde;

    public DictionarySerde(BlockStreamSerde idSerde)
    {
        this.idSerde = idSerde;
    }

    @Override
    public void serialize(BlockStream<? extends ValueBlock> blockStream, SliceOutput sliceOutput)
    {
        checkNotNull(blockStream, "blockStream is null");
        checkNotNull(sliceOutput, "sliceOutput is null");
        checkArgument(blockStream.getTupleInfo().getFieldCount() == 1, "Can only dictionary encode single columns");

        // Generate ID map and write out values
        final BiMap<Slice, Integer> idMap = HashBiMap.create();

        idSerde.serialize(
                new UncompressedBlockStream(
                        blockStream.getTupleInfo(),
                        Iterables.transform(
                                blockStream,
                                new Function<ValueBlock, UncompressedValueBlock>() {
                                    int nextId = 0;
                                    @Override
                                    public UncompressedValueBlock apply(ValueBlock input)
                                    {
                                        // TODO: this is a bit of a hack, fix this later when BlockStream API stabilizes
                                        BlockBuilder blockBuilder = new BlockBuilder(input.getRange().getStart(), TupleInfo.SINGLE_LONG_TUPLEINFO);
                                        for (Tuple tuple : input) {
                                            Integer id = idMap.get(tuple.getTupleSlice());
                                            if (id == null) {
                                                id = nextId;
                                                nextId++;
                                                idMap.put(tuple.getTupleSlice(), id);
                                            }
                                            blockBuilder.append((long) id);
                                        }
                                        return blockBuilder.build();
                                    }
                                }
                        )
                ),
                sliceOutput
        );

        // Convert ID map to compact dictionary array (should be contiguous)
        Slice[] dictionary = new Slice[idMap.size()];
        for (Map.Entry<Integer, Slice> entry : idMap.inverse().entrySet()) {
            dictionary[entry.getKey()] = entry.getValue();
        }

        // Serialize Footer
        int footerBytes = new Footer(blockStream.getTupleInfo(), dictionary).serialize(sliceOutput);

        // Write length of Footer
        sliceOutput.writeInt(footerBytes);
    }

    @Override
    public BlockStream<DictionaryEncodedBlock> deserialize(Slice slice) {
        checkNotNull(slice, "slice is null");

        // Get Footer byte length from tail and reset to beginning
        int footerLen = slice.slice(slice.length() - SizeOf.SIZE_OF_INT, SizeOf.SIZE_OF_INT).input().readInt();

        // Slice out Footer data and extract it
        Slice footerSlice = slice.slice(slice.length() - footerLen - SizeOf.SIZE_OF_INT, footerLen);
        Footer footer = Footer.deserialize(footerSlice);

        Slice payloadSlice = slice.slice(0, slice.length() - footerLen - SizeOf.SIZE_OF_INT);

        return new DictionaryEncodedBlockStream(footer.getTupleInfo(), footer.getDictionary(), idSerde.deserialize(payloadSlice));
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
         * @param sliceOutput
         * @return bytes written to sliceOutput
         */
        private int serialize(SliceOutput sliceOutput)
        {
            int startBytesWriteable = sliceOutput.writableBytes();

            UncompressedTupleInfoSerde.serialize(tupleInfo, sliceOutput);

            sliceOutput.writeInt(dictionary.length);
            for (Slice slice : dictionary) {
                // Write length
                sliceOutput.writeInt(slice.length());
                // Write value
                sliceOutput.writeBytes(slice);
            }

            int endBytesWriteable = sliceOutput.writableBytes();
            return startBytesWriteable - endBytesWriteable;
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
