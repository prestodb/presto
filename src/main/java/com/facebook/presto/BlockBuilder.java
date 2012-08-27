package com.facebook.presto;

import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BlockBuilder
{
    private static final DataSize DEFAULT_MAX_BLOCK_SIZE = new DataSize(64, Unit.KILOBYTE);

    private final long startPosition;
    private final TupleInfo tupleInfo;
    private final int maxBlockSize;
    private final DynamicSliceOutput sliceOutput;
    private int count;

    private TupleInfo.Builder tupleBuilder;

    public BlockBuilder(long startPosition, TupleInfo tupleInfo)
    {
        this(startPosition, tupleInfo, DEFAULT_MAX_BLOCK_SIZE);
    }

    public BlockBuilder(long startPosition, TupleInfo tupleInfo, DataSize blockSize)
    {
        checkArgument(startPosition >= 0, "startPosition is negative");
        checkNotNull(blockSize, "blockSize is null");

        this.startPosition = startPosition;
        this.tupleInfo = tupleInfo;
        maxBlockSize = (int) blockSize.toBytes();
        sliceOutput = new DynamicSliceOutput((int) blockSize.toBytes());

        tupleBuilder = tupleInfo.builder(sliceOutput);
    }

    public boolean isFull()
    {
        return sliceOutput.size() > maxBlockSize;
    }

    public BlockBuilder append(long value)
    {
        flushTupleIfNecessary();

        tupleBuilder.append(value);

        return this;
    }

    public BlockBuilder append(byte[] value)
    {
        flushTupleIfNecessary();

        tupleBuilder.append(Slices.wrappedBuffer(value));

        return this;
    }

    public BlockBuilder append(Slice value)
    {
        flushTupleIfNecessary();

        tupleBuilder.append(value);

        return this;
    }

    public BlockBuilder append(Tuple tuple)
    {
        flushTupleIfNecessary();

        tupleBuilder.append(tuple);

        return this;
    }

    private void flushTupleIfNecessary()
    {
        if (tupleBuilder.isComplete()) {
            tupleBuilder.finish();
            count++;
        }
    }

    public UncompressedValueBlock build()
    {
        flushTupleIfNecessary();

        Preconditions.checkState(count > 0, "Cannot build an empty block");

        return new UncompressedValueBlock(Range.create(startPosition, startPosition + count - 1), tupleInfo, sliceOutput.slice());
    }
}
