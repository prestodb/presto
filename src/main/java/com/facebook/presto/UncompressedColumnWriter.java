package com.facebook.presto;

import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.io.OutputStream;

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedColumnWriter
        extends AbstractColumnProcessor
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();

    private final OutputStream out;

    private int tupleCount = 0;
    private DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE * 2);

    public UncompressedColumnWriter(TupleInfo.Type type, int index, Cursor cursor, OutputStream out)
            throws IOException
    {
        super(type, index, cursor);
        this.out = checkNotNull(out, "out is null");

        writeTupleInfo(out, new TupleInfo(type));
    }

    @Override
    public boolean processPositions(long end)
            throws IOException
    {
        switch (type) {
            case FIXED_INT_64:
                return writeFixedInt64(end);
            case VARIABLE_BINARY:
                return writeVariableBinary(end);
            default:
                throw new AssertionError("unhandled type: " + type);
        }
    }

    @Override
    protected void finished()
            throws IOException
    {
        flush();
    }

    private boolean writeFixedInt64(long end)
            throws IOException
    {
        while (cursor.advanceNextPosition()) {
            buffer.appendLong(cursor.getLong(index));
            tupleCount++;
            flushIfNecessary();
            if (cursor.getPosition() >= end) {
                return true;
            }
        }
        return false;
    }

    private boolean writeVariableBinary(long end)
            throws IOException
    {
        while (cursor.advanceNextPosition()) {
            Slice slice = cursor.getSlice(index);
            buffer.appendShort(slice.length() + SIZE_OF_SHORT);
            buffer.appendBytes(slice);
            tupleCount++;
            flushIfNecessary();
            if (cursor.getPosition() >= end) {
                return true;
            }
        }
        return false;
    }

    private void flushIfNecessary()
            throws IOException
    {
        if (buffer.size() >= MAX_BLOCK_SIZE) {
            flush();
        }
    }

    private void flush()
            throws IOException
    {
        if (tupleCount > 0) {
            write(out, tupleCount, buffer.slice());
            tupleCount = 0;
            buffer.reset();
        }
    }

    private static void writeTupleInfo(OutputStream out, TupleInfo tupleInfo)
            throws IOException
    {
        out.write(tupleInfo.getFieldCount());
        for (TupleInfo.Type type : tupleInfo.getTypes()) {
            out.write(type.ordinal());
        }
    }

    private static void write(OutputStream out, int tupleCount, Slice slice)
            throws IOException
    {
        Slice header = Slices.allocate(SIZE_OF_INT + SIZE_OF_INT);
        header.output()
                .appendInt(slice.length())
                .appendInt(tupleCount);
        header.getBytes(0, out, header.length());

        slice.getBytes(0, out, slice.length());
    }
}
