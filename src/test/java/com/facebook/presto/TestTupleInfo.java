package com.facebook.presto;

import org.testng.annotations.Test;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static org.testng.Assert.assertEquals;

public class TestTupleInfo
{
    @Test
    public void testBasic()
    {
        Slice slice = new Slice(SIZE_OF_LONG + SIZE_OF_SHORT + SIZE_OF_LONG + SIZE_OF_SHORT + SIZE_OF_SHORT + 10 + 15);

        Slice binary1 = Slices.wrappedBuffer(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        Slice binary2 = Slices.wrappedBuffer(new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 });

        slice.output()
                .appendLong(42)
                .appendLong(67)
                .appendShort(22)
                .appendShort(32) // 22 + 10
                .appendShort(47) // 22 + 10 + 15
                .appendBytes(binary1)
                .appendBytes(binary2);


        TupleInfo info = new TupleInfo(FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64, VARIABLE_BINARY);
        assertEquals(info.getLong(slice, 0), 42L);
        assertEquals(info.getSlice(slice, 1), binary1);
        assertEquals(info.getLong(slice, 2), 67L);
        assertEquals(info.getSlice(slice, 3), binary2);
    }
}
