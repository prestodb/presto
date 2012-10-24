package com.facebook.presto.block;

import org.testng.annotations.Test;

public class TestTupleStreamChunker
{
    @Test
    public void testSanity() throws Exception
    {
        TupleStream base = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb");
        Blocks.assertTupleStreamEquals(
                new GenericTupleStream<>(base.getTupleInfo(), TupleStreamChunker.chunk(3, base)),
                base
        );
    }

    @Test
    public void testSinglePositionChunks() throws Exception
    {
        TupleStream base = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb");
        Blocks.assertTupleStreamEquals(
                new GenericTupleStream<>(base.getTupleInfo(), TupleStreamChunker.chunk(1, base)),
                base
        );
    }

    @Test
    public void testExactlyMatchedChunk() throws Exception
    {
        TupleStream base = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb");
        Blocks.assertTupleStreamEquals(
                new GenericTupleStream<>(base.getTupleInfo(), TupleStreamChunker.chunk(8, base)),
                base
        );
    }

    @Test
    public void testLargerChunk() throws Exception
    {
        TupleStream base = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb");
        Blocks.assertTupleStreamEquals(
                new GenericTupleStream<>(base.getTupleInfo(), TupleStreamChunker.chunk(Integer.MAX_VALUE, base)),
                base
        );
    }

    @Test
    public void testOffset() throws Exception
    {
        TupleStream base = Blocks.createTupleStream(5, "a", "bb", "c", "d", "e", "e", "a", "bb");
        Blocks.assertTupleStreamEquals(
                new GenericTupleStream<>(base.getTupleInfo(), TupleStreamChunker.chunk(2, base)),
                base
        );
    }
}
