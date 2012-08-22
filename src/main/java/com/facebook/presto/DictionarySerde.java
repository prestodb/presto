package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;

import java.util.Iterator;
import java.util.Map;

public class DictionarySerde
{
    private final long maxCardinality;
    // TODO: we may be able to determine and adjust this value dynamically with a smarter implementation
    private final int reqBitSpace;
    
    public DictionarySerde(long maxCardinality) {
        this.maxCardinality = maxCardinality;
        reqBitSpace = Long.SIZE - Long.numberOfLeadingZeros(maxCardinality - 1);
    }

    public DictionarySerde() {
        this(Long.MAX_VALUE);
    }

    public void serialize(final Iterable<Slice> slices, SliceOutput sliceOutput)
    {
        final BiMap<Slice, Long> idMap = HashBiMap.create();
        
        PackedLongSerde packedLongSerde = new PackedLongSerde(reqBitSpace);
        packedLongSerde.serialize(
                new Iterable<Long>() {
                    @Override
                    public Iterator<Long> iterator() {
                        return new AbstractIterator<Long>() {
                            Iterator<Slice> sliceIterator = slices.iterator();
                            // Start ID at the smallest possible value to fully utilize available bit space
                            long nextId = -1L << (reqBitSpace - 1);

                            @Override
                            protected Long computeNext() {
                                if (!sliceIterator.hasNext()) {
                                    return endOfData();
                                }

                                Slice slice = sliceIterator.next();

                                Long id = idMap.get(slice);
                                if (id == null) {
                                    id = nextId;
                                    nextId++;
                                    idMap.put(slice, id);
                                }
                                return id;
                            }
                        };
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
                return new AbstractIterator<Slice>() {
                    Iterator<Long> iterator = PackedLongSerde.deserialize(paylodSliceInput).iterator();

                    @Override
                    protected Slice computeNext() {
                        if (!iterator.hasNext()) {
                            return endOfData();
                        }
                        Slice slice = idMap.get(iterator.next());
                        Preconditions.checkNotNull(slice, "Missing entry in dictionary");
                        return slice;
                    }
                };
            }
        };
    }

    // TODO: this encoding can be made more compact if we leverage sorted order of the map
    private static class Footer
    {
        Map<Long, Slice> idMap;

        private Footer(Map<Long, Slice> idMap)
        {
            this.idMap = idMap;
        }

        /**
         * Serialize this Footer to the specified SliceOutput
         * 
         * @param sliceOutput
         * @return bytes written to sliceOutput
         */
        private int serialize(SliceOutput sliceOutput)
        {
            int startBytesWriteable = sliceOutput.writableBytes();
            for (Map.Entry<Long, Slice> entry : idMap.entrySet()) {
                // Write ID number
                sliceOutput.writeLong(entry.getKey());
                // Write Slice length
                sliceOutput.writeInt(entry.getValue().length());
                // Write Slice
                sliceOutput.writeBytes(entry.getValue());
            }
            int endBytesWriteable = sliceOutput.writableBytes();
            return startBytesWriteable - endBytesWriteable;
        }

        private static Footer deserialize(SliceInput sliceInput)
        {
            ImmutableBiMap.Builder<Long, Slice> builder = ImmutableBiMap.builder();

            while (sliceInput.isReadable()) {
                // Read Slice ID number
                long id = sliceInput.readLong();
                // Read Slice Length
                int sliceLength = sliceInput.readInt();
                // Read Slice
                Slice slice = sliceInput.readSlice(sliceLength);

                builder.put(id, slice);
            }

            return new Footer(builder.build());
        }

        public Map<Long, Slice> getIdMap()
        {
            return idMap;
        }
    }
}
