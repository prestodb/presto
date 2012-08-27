/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.io.Closeables;
import com.google.common.io.OutputSupplier;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedBlockSerde
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();

    private UncompressedBlockSerde()
    {
    }

    public static void write(Iterator<ValueBlock> iterator, File file)
            throws IOException
    {
        try (FileOutputStream out = new FileOutputStream(file)) {
            write(iterator, out);
        }
    }

    public static void write(Iterator<ValueBlock> iterator, OutputStream out)
            throws IOException
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
                    writeTupleInfo(out, tupleInfo);
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

    private static void writeTupleInfo(OutputStream out, TupleInfo tupleInfo)
            throws IOException
    {
        out.write(tupleInfo.getFieldCount());
        for (Type type : tupleInfo.getTypes()) {
            out.write(type.ordinal());
        }
    }

    private static void write(OutputStream out, int tupleCount, Slice slice)
            throws IOException
    {
        ByteArraySlice header = Slices.allocate(SIZE_OF_INT + SIZE_OF_INT);
        header.output()
                .appendInt(slice.length())
                .appendInt(tupleCount);
        header.getBytes(0, out, header.length());

        slice.getBytes(0, out, slice.length());
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

            int fieldCount = sliceInput.readUnsignedByte();
            Builder<Type> builder = ImmutableList.builder();
            for (int i = 0; i < fieldCount; i++) {
                builder.add(Type.values()[sliceInput.readUnsignedByte()]);
            }
            this.tupleInfo = new TupleInfo(builder.build());
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

    public static class UncompressedColumnWriter implements ColumnProcessor
    {
        private final OutputSupplier<? extends OutputStream> outputSupplier;
        private final Type type;
        private OutputStream out;

        public UncompressedColumnWriter(OutputSupplier<? extends OutputStream> outputSupplier, Type type)
        {
            this.outputSupplier = outputSupplier;
            this.type = type;
        }

        @Override
        public Type getColumnType()
        {
            return type;
        }

        @Override
        public void processBlock(ValueBlock block)
        {
            try {
                if (out == null) {
                    out = outputSupplier.getOutput();
                    writeTupleInfo(out, new TupleInfo(type));
                }

                Slice blockSlice = ((UncompressedValueBlock) block).getSlice();
                write(out, block.getCount(), blockSlice);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void finish()
        {
            Closeables.closeQuietly(out);
            out = null;
        }
    }

    // TODO: fix this horrible hack
    public static class FloatMillisUncompressedColumnWriter
            extends UncompressedColumnWriter
    {
        public FloatMillisUncompressedColumnWriter(OutputSupplier<? extends OutputStream> outputSupplier, TupleInfo.Type type)
        {
            super(outputSupplier, type);
        }
    }
}
