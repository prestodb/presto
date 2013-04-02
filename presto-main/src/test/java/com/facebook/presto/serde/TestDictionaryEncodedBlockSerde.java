package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.Tuples.createTuple;

public class TestDictionaryEncodedBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        ImmutableList<Tuple> tuples = ImmutableList.of(createTuple("alice"),
                createTuple("bob"),
                createTuple("charlie"),
                createTuple("dave"));

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = new DictionaryEncoder(new UncompressedEncoder(sliceOutput)).append(tuples).append(tuples).append(tuples).finish();
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, new BlockBuilder(SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build());
    }
}
