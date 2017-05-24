package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;

public class TestUncompressedColumnInput
{
    @Test
    public void testIterator()
            throws Exception
    {
        Slice slice = uncompressedLongs(0, 1, 2, 4, 9, 12345);
        UncompressedColumnInput firstColumn = new UncompressedColumnInput(slice, SIZE_OF_LONG);

        ImmutableList<Pair> actual = ImmutableList.copyOf(new PairsIterator(firstColumn.iterator()));
        Assert.assertEquals(actual,
                ImmutableList.of(
                        new Pair(0, createTuple(0)),
                        new Pair(1, createTuple(1)),
                        new Pair(2, createTuple(2)),
                        new Pair(3, createTuple(4)),
                        new Pair(4, createTuple(9)),
                        new Pair(5, createTuple(12345))));
    }

    private Tuple createTuple(long value)
    {
        Slice slice = Slices.allocate(SIZE_OF_LONG);
        slice.setLong(0, value);
        return new Tuple(slice, new TupleInfo(SIZE_OF_LONG));
    }

    private static Slice uncompressedLongs(long... longs)
    {
        ByteArrayDataOutput buffer = ByteStreams.newDataOutput(longs.length * 8);
        for (long v : longs) {
            buffer.write(Longs.toByteArray(Long.reverseBytes(v)));
        }
        return new Slice(buffer.toByteArray());
    }
}
