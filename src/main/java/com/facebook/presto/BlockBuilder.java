package com.facebook.presto;

import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

public class BlockBuilder
{
    private static final DataSize DEFAULT_MAX_BLOCK_SIZE = new DataSize(64, Unit.KILOBYTE);

    private final long startPosition;
    private final TupleInfo tupleInfo;
    private final int maxBlockSize;
    private final DynamicSliceOutput sliceOutput;

    public BlockBuilder(long startPosition, TupleInfo tupleInfo)
    {
        this(startPosition, tupleInfo, DEFAULT_MAX_BLOCK_SIZE);
    }

    public BlockBuilder(long startPosition, TupleInfo tupleInfo, DataSize blockSize)
    {
        Preconditions.checkArgument(startPosition >= 0, "startPosition is negative");
        Preconditions.checkNotNull(blockSize, "blockSize is null");

        this.startPosition = startPosition;
        this.tupleInfo = tupleInfo;
        maxBlockSize = (int) blockSize.toBytes();
        sliceOutput = new DynamicSliceOutput((int) blockSize.toBytes());
    }

    public boolean isFull()
    {
        return sliceOutput.size() > maxBlockSize;
    }

    public void append(byte value)
    {
        sliceOutput.write(value);
    }

    public void append(int value)
    {
        sliceOutput.writeInt(value);
    }

    public void append(long value)
    {
        sliceOutput.writeLong(value);
    }

    public void append(byte[] value)
    {
        sliceOutput.writeBytes(value);
    }

    public void append(Slice value)
    {
        sliceOutput.writeBytes(value);
    }

    public void append(Tuple tuple)
    {
        tuple.writeTo(sliceOutput);
    }

    public UncompressedValueBlock build()
    {
        return new UncompressedValueBlock(startPosition, tupleInfo, sliceOutput.slice());
    }
}
