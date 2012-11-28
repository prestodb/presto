package com.facebook.presto.block;

import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BlockBuilder
{
    public static final DataSize DEFAULT_MAX_BLOCK_SIZE = new DataSize(64, Unit.KILOBYTE);
    public static final double DEFAULT_STORAGE_MULTIPLIER = 1.2;

    private final TupleInfo tupleInfo;
    private final int maxBlockSize;
    private final SliceOutput sliceOutput;
    private int count;

    private TupleInfo.Builder tupleBuilder;

    public BlockBuilder(TupleInfo tupleInfo)
    {
        this(tupleInfo, DEFAULT_MAX_BLOCK_SIZE, DEFAULT_STORAGE_MULTIPLIER);
    }

    public BlockBuilder(TupleInfo tupleInfo, DataSize blockSize, double storageMultiplier)
    {
        // Use slightly larger storage size to minimize resizing when we just exceed full capacity
        this(tupleInfo, (int) checkNotNull(blockSize, "blockSize is null").toBytes(), new DynamicSliceOutput((int) ((int) blockSize.toBytes() * storageMultiplier)));
    }

    public BlockBuilder(TupleInfo tupleInfo,
            int maxBlockSize,
            SliceOutput sliceOutput)
    {
        checkNotNull(maxBlockSize, "maxBlockSize is null");
        checkNotNull(tupleInfo, "tupleInfo is null");

        this.tupleInfo = tupleInfo;
        this.maxBlockSize = maxBlockSize;
        this.sliceOutput = sliceOutput;

        tupleBuilder = tupleInfo.builder(this.sliceOutput);
    }

    public boolean isEmpty()
    {
        checkState(!tupleBuilder.isPartial(), "Tuple is not complete");
        return count == 0;
    }

    public boolean isFull()
    {
        checkState(!tupleBuilder.isPartial(), "Tuple is not complete");
        return sliceOutput.size() > maxBlockSize;
    }

    public BlockBuilder append(long value)
    {
        tupleBuilder.append(value);
        flushTupleIfNecessary();
        return this;
    }

    public BlockBuilder append(double value)
    {
        tupleBuilder.append(value);
        flushTupleIfNecessary();
        return this;
    }

    public BlockBuilder append(byte[] value)
    {
        return append(Slices.wrappedBuffer(value));
    }

    public BlockBuilder append(String value)
    {
        return append(Slices.copiedBuffer(value, Charsets.UTF_8));
    }

    public BlockBuilder append(Slice value)
    {
        tupleBuilder.append(value);
        flushTupleIfNecessary();
        return this;
    }

    public BlockBuilder appendNull()
    {
        tupleBuilder.appendNull();
        flushTupleIfNecessary();
        return this;
    }

    public BlockBuilder append(Tuple tuple)
    {
        tupleBuilder.append(tuple);
        flushTupleIfNecessary();
        return this;
    }

    public BlockBuilder appendTuple(Slice slice, int offset)
    {
        Preconditions.checkState(!tupleBuilder.isPartial());

        // read the tuple length
        int length = tupleInfo.size(slice, offset);
        return appendTuple(slice, offset, length);


    }

    public BlockBuilder appendTuple(Slice slice, int offset, int length)
    {
        Preconditions.checkState(!tupleBuilder.isPartial());

        // copy tuple to output
        sliceOutput.writeBytes(slice, offset, length);
        count++;

        return this;
    }

    private void flushTupleIfNecessary()
    {
        if (tupleBuilder.isComplete()) {
            tupleBuilder.finish();
            count++;
        }
    }

    public UncompressedBlock build()
    {
        checkState(!tupleBuilder.isPartial(), "Tuple is not complete");
        checkState(!isEmpty(), "Cannot build an empty block");

        return new UncompressedBlock(count, tupleInfo, sliceOutput.slice());
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("BlockBuilder");
        sb.append("{count=").append(count);
        sb.append(", size=").append(sliceOutput.size());
        sb.append(", maxSize=").append(maxBlockSize);
        sb.append(", tupleInfo=").append(tupleInfo);
        sb.append('}');
        return sb.toString();
    }
}
