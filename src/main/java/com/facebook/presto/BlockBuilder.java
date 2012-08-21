package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ranges;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.*;

public class BlockBuilder
{
    private static final DataSize DEFAULT_MAX_BLOCK_SIZE = new DataSize(64, Unit.KILOBYTE);

    private final long startPosition;
    private final TupleInfo tupleInfo;
    private final int maxBlockSize;
    private final DynamicSliceOutput sliceOutput;
    private int count;

    private final int fixedPartSize;

    private int currentField;
    private final List<Slice> variableLengthFields;

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

        int fixedPartSize = 0;
        int variableLengthFieldCount = 0;
        for (TupleInfo.Type type : tupleInfo.getTypes()) {
            if (!type.isFixedSize()) {
                variableLengthFieldCount++;
                fixedPartSize += SizeOf.SIZE_OF_SHORT;
            }
            else {
                fixedPartSize += type.getSize();
            }
        }
        fixedPartSize += SizeOf.SIZE_OF_SHORT; // total tuple size

        this.fixedPartSize = fixedPartSize;
        variableLengthFields = new ArrayList<>(variableLengthFieldCount);
    }

    public boolean isFull()
    {
        return sliceOutput.size() > maxBlockSize;
    }

    public void append(long value)
    {
        flushTupleIfNecessary();

        checkState(tupleInfo.getTypes().get(currentField) == FIXED_INT_64, "Expected FIXED_INT_64");

        sliceOutput.writeLong(value);
        currentField++;
    }

    public void append(byte[] value)
    {
        flushTupleIfNecessary();

        checkState(tupleInfo.getTypes().get(currentField) == VARIABLE_BINARY, "Expected VARIABLE_BINARY");

        variableLengthFields.add(Slices.wrappedBuffer(value));
        currentField++;
    }

    public void append(Slice value)
    {
        flushTupleIfNecessary();

        checkState(tupleInfo.getTypes().get(currentField) == VARIABLE_BINARY, "Expected VARIABLE_BINARY");

        variableLengthFields.add(value);
        currentField++;
    }

    public void append(Tuple tuple)
    {
        // TODO: optimization - single copy of block of fixed length fields

        int index = 0;
        for (TupleInfo.Type type : tuple.getTupleInfo().getTypes()) {
            switch (type) {
                case FIXED_INT_64:
                    append(tuple.getLong(index));
                    break;
                case VARIABLE_BINARY:
                    append(tuple.getSlice(index));
                    break;
                default:
                    throw new IllegalStateException("Type not yet supported: " + type);
            }

            index++;
        }
    }

    private void flushTupleIfNecessary()
    {
        if (currentField < tupleInfo.getTypes().size()) {
            return;
        }

        // write offsets
        int offset = fixedPartSize;
        for (Slice field : variableLengthFields) {
            sliceOutput.writeShort(offset);
            offset += field.length();
        }

        if (!variableLengthFields.isEmpty()) {
            sliceOutput.writeShort(offset); // total tuple length
        }

        // write values
        for (Slice field : variableLengthFields) {
            sliceOutput.writeBytes(field);
        }

        count++;
        currentField = 0;
        variableLengthFields.clear();
    }

    public ValueBlock build()
    {
        flushTupleIfNecessary();

        Preconditions.checkState(currentField == 0, "Last tuple is incomplete");

        if (count == 0) {
            return EmptyValueBlock.INSTANCE;
        }

        return new UncompressedValueBlock(Ranges.closed(startPosition, startPosition + count - 1), tupleInfo, sliceOutput.slice());
    }
}
