/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStreamWriter;
import com.facebook.presto.slice.ByteArraySlice;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.slice.Slices;
import com.google.common.collect.AbstractIterator;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedSerde
        implements TupleStreamSerde
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();
    private static final UncompressedSerde INSTANCE = new UncompressedSerde();

    @Override
    public void serialize(TupleStream tupleStream, SliceOutput sliceOutput)
    {
        createTupleStreamWriter(sliceOutput)
                .append(tupleStream)
                .finished();
    }

    @Override
    public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
    {
        return new UncompressedTupleStreamWriter(sliceOutput);
    }

    @Override
    public TupleStream deserialize(Slice slice)
    {
        return readAsStream(slice);
    }

    private static void write(SliceOutput destination, int tupleCount, Slice slice)
    {
        ByteArraySlice header = Slices.allocate(SIZE_OF_INT + SIZE_OF_INT);
        header.output()
                .appendInt(slice.length())
                .appendInt(tupleCount);
        destination.writeBytes(header);
        destination.writeBytes(slice);
    }

    public static TupleStream read(File file)
            throws IOException
    {
        Slice mappedSlice = Slices.mapFileReadOnly(file);
        return INSTANCE.deserialize(mappedSlice);
    }

    public static Iterator<UncompressedBlock> read(Slice slice)
    {
        return new UncompressedReader(slice);
    }

    public static TupleStream readAsStream(final Slice slice)
    {
        UncompressedReader reader = new UncompressedReader(slice);

        return new UncompressedTupleStream(reader.tupleInfo, new Iterable<UncompressedBlock>()
        {
            @Override
            public Iterator<UncompressedBlock> iterator()
            {
                return new UncompressedReader(slice);
            }
        });
    }

    private static class UncompressedTupleStreamWriter
            implements TupleStreamWriter
    {
        private final SliceOutput sliceOutput;

        private boolean initialized = false;
        private boolean finished = false;
        private DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);
        private int tupleCount = 0;

        private UncompressedTupleStreamWriter(SliceOutput sliceOutput)
        {
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
        }

        @Override
        public TupleStreamWriter append(TupleStream tupleStream)
        {
            checkNotNull(tupleStream, "tupleStream is null");
            checkState(!finished, "already finished");

            if (!initialized) {
                // todo We should be able to take advantage of the fact that the cursor might already be over uncompressed blocks and just write them down as they come.
                UncompressedTupleInfoSerde.serialize(tupleStream.getTupleInfo(), sliceOutput);
                initialized = true;
            }

            Cursor cursor = tupleStream.cursor();

            while (cursor.advanceNextPosition()) {
                cursor.getTuple().writeTo(buffer);
                tupleCount++;

                if (buffer.size() > MAX_BLOCK_SIZE) {
                    write(sliceOutput, tupleCount, buffer.slice());
                    tupleCount = 0;
                    buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);
                }
            }

            return this;
        }

        @Override
        public void finished()
        {
            checkState(initialized, "nothing appended");
            checkState(!finished, "already finished");
            finished = true;

            if (buffer.size() > 0) {
                write(sliceOutput, tupleCount, buffer.slice());
            }
        }
    }

    private static class UncompressedReader
            extends AbstractIterator<UncompressedBlock>
    {
        private final TupleInfo tupleInfo;
        private final SliceInput sliceInput;
        private int position;

        private UncompressedReader(Slice slice)
        {
            sliceInput = slice.input();
            this.tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);
        }

        protected UncompressedBlock computeNext()
        {
            if (!sliceInput.isReadable()) {
                endOfData();
                return null;
            }

            int blockSize = sliceInput.readInt();
            int tupleCount = sliceInput.readInt();

            Range range = Range.create(position, position + tupleCount - 1);
            position += tupleCount;

            Slice block = sliceInput.readSlice(blockSize);
            return new UncompressedBlock(range, tupleInfo, block);
        }
    }
}
