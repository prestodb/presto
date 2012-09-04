package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionarySerde
{
    // TODO: we may be able to determine and adjust this value dynamically with a smarter implementation
    private final int requiredBitSpace;

    public DictionarySerde(long maxCardinality) {
        checkArgument(maxCardinality >= 2, "maxCardinality should be at least 2");
        // Faster way to compute ceil(log2(maxCardinality))
        requiredBitSpace = Long.SIZE - Long.numberOfLeadingZeros(maxCardinality - 1);
    }

    public DictionarySerde() {
        this(Long.MAX_VALUE);
    }

    public void serialize(final Iterable<Slice> slices, SliceOutput sliceOutput)
    {
        final BiMap<Slice, Long> idMap = HashBiMap.create();

        PackedLongSerde packedLongSerde = new PackedLongSerde(requiredBitSpace);
        packedLongSerde.serialize(
                new Iterable<Long>() {
                    @Override
                    public Iterator<Long> iterator() {
                        return Iterators.transform(
                                slices.iterator(),
                                new Function<Slice, Long>() {
                                    long nextId = -1L << (requiredBitSpace - 1);

                                    @Override
                                    public Long apply(Slice input) {
                                        Long id = idMap.get(input);
                                        if (id == null) {
                                            id = nextId;
                                            nextId++;
                                            idMap.put(input, id);
                                        }
                                        return id;
                                    }
                                }
                        );
                    }
                },
                sliceOutput
        );

        // Serialize Footer
        int footerBytes = new Footer(idMap.inverse()).serialize(sliceOutput);

        // Write length of Footer
        sliceOutput.writeInt(footerBytes);
    }

    public static Iterable<Slice> deserialize(final SliceInput sliceInput) {
        // Get map serialized byte length from tail and reset to beginning
        int totalBytes = sliceInput.available();
        sliceInput.skipBytes(totalBytes - SizeOf.SIZE_OF_INT);
        int idMapByteLength = sliceInput.readInt();

        // Slice out Footer data and extract it
        sliceInput.setPosition(totalBytes - idMapByteLength - SizeOf.SIZE_OF_INT);
        Footer footer = Footer.deserialize(sliceInput.readSlice(idMapByteLength).input());

        final Map<Long, Slice> idMap = footer.getIdMap();

        sliceInput.setPosition(0);
        final SliceInput paylodSliceInput =
                sliceInput.readSlice(totalBytes - idMapByteLength - SizeOf.SIZE_OF_INT)
                        .input();
        return new Iterable<Slice>() {
            @Override
            public Iterator<Slice> iterator() {
                return Iterators.transform(
                        PackedLongSerde.deserialize(paylodSliceInput).iterator(),
                        new Function<Long, Slice>() {
                            @Override
                            public Slice apply(Long input) {
                                Slice slice = idMap.get(input);
                                checkNotNull(slice, "Missing entry in dictionary");
                                return slice;
                            }
                        }
                );
            }
        };
    }

    // TODO: this encoding can be made more compact if we leverage sorted order of the map
    private static class Footer
    {
        private final Map<Long, Slice> idMap;

        private Footer(Map<Long, Slice> idMap)
        {
            this.idMap = ImmutableMap.copyOf(idMap);
        }

        public Map<Long, Slice> getIdMap()
        {
            return idMap;
        }

        /**
         * @param sliceOutput
         * @return bytes written to sliceOutput
         */
        private int serialize(SliceOutput sliceOutput)
        {
            int startBytesWriteable = sliceOutput.writableBytes();
            for (Map.Entry<Long, Slice> entry : idMap.entrySet()) {
                // Write ID number
                sliceOutput.writeLong(entry.getKey());
                // Write length
                sliceOutput.writeInt(entry.getValue().length());
                // Write value
                sliceOutput.writeBytes(entry.getValue());
            }
            int endBytesWriteable = sliceOutput.writableBytes();
            return startBytesWriteable - endBytesWriteable;
        }

        private static Footer deserialize(SliceInput sliceInput)
        {
            ImmutableMap.Builder<Long, Slice> builder = ImmutableMap.builder();

            while (sliceInput.isReadable()) {
                // Read value ID number
                long id = sliceInput.readLong();
                // Read value Length
                int sliceLength = sliceInput.readInt();
                // Read value
                Slice slice = sliceInput.readSlice(sliceLength);

                builder.put(id, slice);
            }

            return new Footer(builder.build());
        }
    }
}
