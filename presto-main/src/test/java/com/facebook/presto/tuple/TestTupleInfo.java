package com.facebook.presto.tuple;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTupleInfo
{
    @Test
    public void testOnlyFixedLength()
    {
        TupleInfo info = new TupleInfo(FIXED_INT_64, FIXED_INT_64);

        Tuple tuple = info.builder()
                .append(42)
                .append(67)
                .build();

        assertEquals(tuple.getLong(0), 42L);
        assertEquals(tuple.getLong(1), 67L);
        assertEquals(tuple.size(), 16);
    }

    /**
     * The following classes depend on this exact memory layout
     * @see com.facebook.presto.block.uncompressed.UncompressedLongBlockCursor
     * @see com.facebook.presto.block.uncompressed.UncompressedBlock
     */
    @Test
    public void testOnlyFixedLengthMemoryLayout()
    {
        TupleInfo info = new TupleInfo(FIXED_INT_64, FIXED_INT_64);

        Tuple tuple = info.builder()
                .append(42)
                .append(67)
                .build();

        Slice tupleSlice = tuple.getTupleSlice();
        assertEquals(tupleSlice.length(), 16);
        assertEquals(tupleSlice.getLong(0), 42L);
        assertEquals(tupleSlice.getLong(SIZE_OF_LONG), 67L);
    }

    @Test
    public void testSingleVariableLength()
    {
        Slice binary = Slices.wrappedBuffer(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

        Tuple tuple = TupleInfo.SINGLE_VARBINARY.builder()
                .append(binary)
                .build();

        assertEquals(tuple.getSlice(0), binary);
        assertEquals(tuple.size(), binary.length() + SIZE_OF_INT);
    }

    /**
     * The following classes depend on this exact memory layout
     * @see com.facebook.presto.block.uncompressed.UncompressedSliceBlockCursor
     * @see com.facebook.presto.block.uncompressed.UncompressedBlock
     */
    @Test
    public void testSingleVariableLengthMemoryLayout()
    {
        Slice binary = Slices.wrappedBuffer(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

        Tuple tuple = TupleInfo.SINGLE_VARBINARY.builder()
                .append(binary)
                .build();

        Slice tupleSlice = tuple.getTupleSlice();
        assertEquals(tupleSlice.length(), binary.length() + SIZE_OF_INT);
        assertEquals(tupleSlice.getInt(0), binary.length() + SIZE_OF_INT);
        assertEquals(tupleSlice.slice(SIZE_OF_INT, binary.length()), binary);
    }

    @Test
    public void testSingleVariableLengthNull()
    {
        Tuple tuple = TupleInfo.SINGLE_VARBINARY.builder()
                .appendNull()
                .build();

        assertTrue(tuple.isNull(0));
        assertEquals(tuple.getSlice(0), Slices.EMPTY_SLICE);
        assertEquals(tuple.size(), SIZE_OF_INT);
    }

    /**
     * The following classes depend on this exact memory layout
     * @see com.facebook.presto.block.uncompressed.UncompressedSliceBlockCursor
     * @see com.facebook.presto.block.uncompressed.UncompressedBlock
     */
    @Test
    public void testSingleVariableLengthNullMemoryLayout()
    {
        Tuple tuple = TupleInfo.SINGLE_VARBINARY.builder()
                .appendNull()
                .build();

        Slice tupleSlice = tuple.getTupleSlice();
        assertEquals(tupleSlice.length(), SIZE_OF_INT);
        // the size of the tuple is stored in the first int
        // value should SIZE_OF_INT with the high bit set
        assertEquals(tupleSlice.getInt(0), SIZE_OF_INT | 0x80_00_00_00);
    }

    @Test
    public void testMultipleVariableLength()
    {
        TupleInfo info = new TupleInfo(VARIABLE_BINARY, VARIABLE_BINARY);

        Slice binary1 = Slices.wrappedBuffer(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        Slice binary2 = Slices.wrappedBuffer(new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 });

        Tuple tuple = info.builder()
                .append(binary1)
                .append(binary2)
                .build();

        assertEquals(tuple.getSlice(0), binary1);
        assertEquals(tuple.getSlice(1), binary2);
        assertEquals(tuple.size(), binary1.length() + binary2.length() + SIZE_OF_INT + SIZE_OF_INT);
    }


    @Test
    public void testMixed()
    {
        TupleInfo info = new TupleInfo(FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64, VARIABLE_BINARY);

        Slice binary1 = Slices.wrappedBuffer(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        Slice binary2 = Slices.wrappedBuffer(new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 });
        Slice binary3 = Slices.wrappedBuffer(new byte[] { 30, 31, 32, 33, 34, 35 });

        Tuple tuple = info.builder()
                .append(42)
                .append(binary1)
                .append(67)
                .append(binary2)
                .append(90)
                .append(binary3)
                .build();

        assertEquals(tuple.getLong(0), 42L);
        assertEquals(tuple.getSlice(1), binary1);
        assertEquals(tuple.getLong(2), 67L);
        assertEquals(tuple.getSlice(3), binary2);
        assertEquals(tuple.getLong(4), 90L);
        assertEquals(tuple.getSlice(5), binary3);
    }
}
