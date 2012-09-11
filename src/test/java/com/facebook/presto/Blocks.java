package com.facebook.presto;

import org.testng.Assert;

import static com.facebook.presto.Cursors.toPairList;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;

public class Blocks
{
    public static void assertBlockStreamEquals(BlockStream<?> actual, BlockStream<?> expected)
    {
        Assert.assertEquals(actual.getTupleInfo(), expected.getTupleInfo());
        Assert.assertEquals(toPairList(actual.cursor()), toPairList(expected.cursor()));
    }

    public static UncompressedBlockStream createBlockStream(int position, String... values)
    {
        return new UncompressedBlockStream(new TupleInfo(VARIABLE_BINARY), createBlock(position, values));
    }

    public static UncompressedValueBlock createBlock(long position, String... values)
    {
        BlockBuilder builder = new BlockBuilder(position, new TupleInfo(VARIABLE_BINARY));

        for (String value : values) {
            builder.append(value.getBytes(UTF_8));
        }

        return builder.build();
    }

    public static UncompressedBlockStream createBlockStream(int position, long... values)
    {
        return new UncompressedBlockStream(new TupleInfo(FIXED_INT_64), createBlock(position, values));
    }

    public static UncompressedValueBlock createBlock(long position, long... values)
    {
        BlockBuilder builder = new BlockBuilder(position, new TupleInfo(FIXED_INT_64));

        for (long value : values) {
            builder.append(value);
        }

        return builder.build();
    }
}
