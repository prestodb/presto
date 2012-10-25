package com.facebook.presto.block;

import com.facebook.presto.TupleInfo;
import com.google.common.base.Charsets;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;

public class TestStaticTupleAppendingTupleStream
{
    @Test
    public void testAppendSingleColumn()
            throws Exception
    {
        StaticTupleAppendingTupleStream staticTupleAppendingTupleStream = new StaticTupleAppendingTupleStream(
                Blocks.createBlock(0, "abc", "ddd", "fff"),
                SINGLE_LONG.builder().append(5).build()
        );
        Blocks.assertTupleStreamEquals(
                staticTupleAppendingTupleStream,
                new BlockBuilder(0, new TupleInfo(VARIABLE_BINARY, FIXED_INT_64))
                        .append("abc".getBytes(Charsets.UTF_8)).append(5)
                        .append("ddd".getBytes(Charsets.UTF_8)).append(5)
                        .append("fff".getBytes(Charsets.UTF_8)).append(5)
                        .build()
        );
    }

    @Test
    public void testAppendMultiColumn()
            throws Exception
    {
        StaticTupleAppendingTupleStream staticTupleAppendingTupleStream = new StaticTupleAppendingTupleStream(
                Blocks.createBlock(0, "abc", "ddd", "fff"),
                new TupleInfo(FIXED_INT_64, FIXED_INT_64).builder().append(5).append(-1).build()
        );
        Blocks.assertTupleStreamEquals(
                staticTupleAppendingTupleStream,
                new BlockBuilder(0, new TupleInfo(VARIABLE_BINARY, FIXED_INT_64, FIXED_INT_64))
                        .append("abc".getBytes(Charsets.UTF_8)).append(5).append(-1)
                        .append("ddd".getBytes(Charsets.UTF_8)).append(5).append(-1)
                        .append("fff".getBytes(Charsets.UTF_8)).append(5).append(-1)
                        .build()
        );
    }
}
