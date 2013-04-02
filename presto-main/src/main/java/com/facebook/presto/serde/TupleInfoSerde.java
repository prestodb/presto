package com.facebook.presto.serde;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;

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
