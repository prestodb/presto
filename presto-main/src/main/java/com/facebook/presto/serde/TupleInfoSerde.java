package com.facebook.presto.serde;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

// TODO: give this a real implementation, this is just a hack for now
public final class TupleInfoSerde
{
    private TupleInfoSerde()
    {
    }

    public static void writeTupleInfo(SliceOutput sliceOutput, TupleInfo tupleInfo)
    {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(sliceOutput, "sliceOutput is null");

        sliceOutput.writeByte(tupleInfo.getFieldCount());

        for (TupleInfo.Type type : tupleInfo.getTypes()) {
            sliceOutput.writeByte(type.ordinal());
        }
    }

    public static TupleInfo readTupleInfo(SliceInput sliceInput)
    {
        checkNotNull(sliceInput, "sliceInput is null");

        int fieldCount = sliceInput.readUnsignedByte();
        ImmutableList.Builder<TupleInfo.Type> builder = ImmutableList.builder();
        for (int i = 0; i < fieldCount; i++) {
            builder.add(TupleInfo.Type.values()[sliceInput.readUnsignedByte()]);
        }
        return new TupleInfo(builder.build());
    }
}
