/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.operator.OutputProcessor.OutputHandler;
import static com.facebook.presto.operator.OutputProcessor.OutputStats;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestOutputProcessor
{
    private final Block valueBlock;

    public TestOutputProcessor()
    {
        BlockBuilder builder = new BlockBuilder(new TupleInfo(FIXED_INT_64, VARIABLE_BINARY));
        builder.append(0);
        builder.append("zero".getBytes(UTF_8));
        builder.append(1);
        builder.append("one".getBytes(UTF_8));
        builder.append(2);
        builder.append("two".getBytes(UTF_8));
        valueBlock = builder.build();
    }

    @Test
    public void testOutputProcessor()
    {
        Operator source = createOperator(new Page(valueBlock));
        CollectingHandler handler = new CollectingHandler();
        OutputProcessor processor = new OutputProcessor(source, handler);

        OutputStats stats = processor.process();

        assertEquals(handler.getRows(), list(list(0L, "zero"), list(1L, "one"), list(2L, "two")));
        assertEquals(stats.getRows(), 3);
        assertEquals(stats.getBytes(), 49);
    }

    @Test
    public void testOutputProcessorMultipleChannels()
    {
        Operator source = createOperator(new Page(valueBlock, valueBlock, valueBlock));
        CollectingHandler handler = new CollectingHandler();
        OutputProcessor processor = new OutputProcessor(source, handler);

        OutputStats stats = processor.process();

        assertEquals(handler.getRows(), list(
                list(0L, "zero", 0L, "zero", 0L, "zero"),
                list(1L, "one", 1L, "one", 1L, "one"),
                list(2L, "two", 2L, "two", 2L, "two")));
        assertEquals(stats.getRows(), 3);
        assertEquals(stats.getBytes(), 147);
    }

    private static class CollectingHandler
            implements OutputHandler
    {
        private final List<List<Object>> rows = new ArrayList<>();

        @Override
        public void process(List<Object> values)
        {
            rows.add(values);
        }

        public List<List<Object>> getRows()
        {
            return rows;
        }
    }

    private static List<Object> list(Object... objects)
    {
        return ImmutableList.copyOf(objects);
    }
}
