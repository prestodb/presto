package com.facebook.presto;

import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

// TODO: give this a real implementation, this is just a hack for now
public class UncompressedTupleInfoSerde
{
    /**
     *
     * @param tupleInfo
     * @param sliceOutput
     * @return number of bytes written
     */
    public static int serialize(TupleInfo tupleInfo, SliceOutput sliceOutput) {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(sliceOutput, "sliceOutput is null");

        sliceOutput.writeByte(tupleInfo.getFieldCount());
        int bytes = 1;

        for (TupleInfo.Type type : tupleInfo.getTypes()) {
            sliceOutput.writeByte(type.ordinal());
            bytes++;
        }
        return bytes;
    }
    
    public static TupleInfo deserialize(SliceInput sliceInput) {
        checkNotNull(sliceInput, "slice is null");

        int fieldCount = sliceInput.readUnsignedByte();
        ImmutableList.Builder<TupleInfo.Type> builder = ImmutableList.builder();
        for (int i = 0; i < fieldCount; i++) {
            builder.add(TupleInfo.Type.values()[sliceInput.readUnsignedByte()]);
        }
        return new TupleInfo(builder.build());
    }
}
