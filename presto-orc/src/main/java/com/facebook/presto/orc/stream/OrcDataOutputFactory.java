package com.facebook.presto.orc.stream;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static java.util.Objects.requireNonNull;

public class OrcDataOutputFactory
{
    public static OrcDataOutput createDataOutput(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        return new OrcDataOutput()
        {
            @Override
            public long size()
            {
                return slice.length();
            }

            @Override
            public void writeData(SliceOutput sliceOutput)
            {
                sliceOutput.writeBytes(slice);
            }
        };
    }
}
