/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.slice.ByteArraySlice;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.OutputStreamSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedBlockSerde
        implements BlockStreamSerde<UncompressedValueBlock>
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();

    @Override
    public void serialize(BlockStream blockStream, SliceOutput sliceOutput)
    {
        write(blockStream.iterator(), sliceOutput);
    }

    @Override
    public BlockStream<UncompressedValueBlock> deserialize(Slice slice)
    {
        return readAsStream(slice);
    }

    public static void write(Iterator<ValueBlock> iterator, File file)
            throws IOException
    {
        try (FileOutputStream out = new FileOutputStream(file)) {
            write(iterator, out);
        }
    }

    public static void write(Iterator<ValueBlock> iterator, SliceOutput out)
    {
        DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);

        int tupleCount = 0;
        TupleInfo tupleInfo = null;
        while (iterator.hasNext()) {
            ValueBlock valueBlock = iterator.next();
            for (Tuple tuple : valueBlock) {
                if (tupleInfo == null) {
                    // write the tuple info
                    tupleInfo = tuple.getTupleInfo();
                    UncompressedTupleInfoSerde.serialize(tupleInfo, new OutputStreamSliceOutput(out));
                }
                else {
                    Preconditions.checkState(tupleInfo.equals(tuple.getTupleInfo()), "Expected %s, but got %s", tupleInfo, tuple.getTupleInfo());
                }
                tuple.writeTo(buffer);
                tupleCount++;

                if (buffer.size() > MAX_BLOCK_SIZE) {
                    write(out, tupleCount, buffer.slice());
                    tupleCount = 0;
                    buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);
                }
            }
        }
        if (buffer.size() > 0) {
            write(out, tupleCount, buffer.slice());
        }
    }

    public static void write(Iterator<ValueBlock> iterator, OutputStream out)
    {
        write(iterator, new OutputStreamSliceOutput(out));
    }

    private static void write(OutputStream out, int tupleCount, Slice slice)
    {
        write(new OutputStreamSliceOutput(out), tupleCount, slice);
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

    public static Iterator<UncompressedValueBlock> read(File file)
            throws IOException
    {
        Slice mappedSlice = Slices.mapFileReadOnly(file);
        return read(mappedSlice);
    }

    public static Iterator<UncompressedValueBlock> read(Slice slice)
    {
        return new UncompressedReader(slice);
    }

    public static BlockStream<UncompressedValueBlock> readAsStream(final Slice slice)
    {
        UncompressedReader reader = new UncompressedReader(slice);

        return new UncompressedBlockStream(reader.tupleInfo, new Iterable<UncompressedValueBlock>()
        {
            @Override
            public Iterator<UncompressedValueBlock> iterator()
            {
                return new UncompressedReader(slice);
            }
        });
    }

    private static class UncompressedReader extends AbstractIterator<UncompressedValueBlock>
    {
        private final TupleInfo tupleInfo;
        private final SliceInput sliceInput;
        private int position;

        private UncompressedReader(Slice slice)
        {
            sliceInput = slice.input();
            this.tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);
        }

        protected UncompressedValueBlock computeNext()
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
            return new UncompressedValueBlock(range, tupleInfo, block);
        }
    }
}
