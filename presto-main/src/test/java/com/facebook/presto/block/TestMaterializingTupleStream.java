package com.facebook.presto.block;

import io.airlift.units.DataSize;
import org.testng.annotations.Test;

public class TestMaterializingTupleStream
{
    @Test
    public void testSingleChunk() throws Exception
    {
        TupleStream input = Blocks.createBlock(0, "aa", "bb", "cc");
        MaterializingTupleStream materializingTupleStream = new MaterializingTupleStream(
                input,
                DataSize.valueOf("100B"),
                1.2
        );
        Blocks.assertTupleStreamEquals(
                new GenericTupleStream<>(materializingTupleStream.getTupleInfo(), materializingTupleStream),
                input
        );
    }
    
    @Test
    public void testMultipleChunks() throws Exception
    {
        TupleStream input = Blocks.createBlock(0, "aa", "bb", "cc");
        MaterializingTupleStream materializingTupleStream = new MaterializingTupleStream(
                input,
                DataSize.valueOf("1B"),
                1.0
        );
        Blocks.assertTupleStreamEquals(
                new GenericTupleStream<>(materializingTupleStream.getTupleInfo(), materializingTupleStream),
                input
        );
    }
}
