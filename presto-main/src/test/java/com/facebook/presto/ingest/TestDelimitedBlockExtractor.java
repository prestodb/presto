package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.YieldingIterators;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.GenericCursor;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;

public class TestDelimitedBlockExtractor
{
    private DelimitedBlockExtractor blockExtractor;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception
    {
        blockExtractor = new DelimitedBlockExtractor(
                Splitter.on(','),
                ImmutableList.<DelimitedBlockExtractor.ColumnDefinition>of(
                        new DelimitedBlockExtractor.ColumnDefinition(0, VARIABLE_BINARY),
                        new DelimitedBlockExtractor.ColumnDefinition(2, FIXED_INT_64)
                )
        );
    }

    @Test
    public void testExtraction() throws Exception
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY, FIXED_INT_64);
        Iterator<UncompressedBlock> blocks = blockExtractor.extract(ImmutableList.<String>of("apple,fuu,123", "banana,bar,456").iterator());
        Blocks.assertCursorsEquals(
                new GenericCursor(new QuerySession(), tupleInfo, YieldingIterators.yieldingIterator(blocks)),
                Blocks.tupleStreamBuilder(tupleInfo)
                        .append("apple").append(123L)
                        .append("banana").append(456L)
                        .build()
                        .cursor(new QuerySession())

        );
    }
}
