package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Blocks;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

public class TestDelimitedTupleStream
{
    @Test
    public void testSingleColumn() throws Exception
    {
        List<String> input = ImmutableList.of("abc","def","g");
        DelimitedTupleStream delimitedTupleStream = new DelimitedTupleStream(input.iterator(), Splitter.on(","), TupleInfo.SINGLE_VARBINARY);
        Blocks.assertTupleStreamEquals(delimitedTupleStream, Blocks.createBlock(0, "abc", "def", "g"));
    }

    @Test
    public void testMultiColumn() throws Exception
    {
        List<String> input = ImmutableList.of("abc,1","def,2","g,0");
        TupleInfo info = new TupleInfo(TupleInfo.Type.VARIABLE_BINARY, TupleInfo.Type.FIXED_INT_64);
        DelimitedTupleStream delimitedTupleStream = new DelimitedTupleStream(input.iterator(), Splitter.on(","), info);
        Blocks.assertTupleStreamEquals(
                delimitedTupleStream,
                Blocks.tupleStreamBuilder(0, info)
                        .append("abc").append(1)
                        .append("def").append(2)
                        .append("g").append(0)
                        .build()
        );
    }
}
