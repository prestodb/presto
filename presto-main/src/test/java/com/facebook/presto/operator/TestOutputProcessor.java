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
import static com.facebook.presto.operator.OutputProcessor.processOutput;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
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
        processOutput(source, handler);

        assertEquals(handler.getRows(), list(list(0L, "zero"), list(1L, "one"), list(2L, "two")));
    }

    @Test
    public void testOutputProcessorMultipleChannels()
    {
        Operator source = createOperator(new Page(valueBlock, valueBlock, valueBlock));
        CollectingHandler handler = new CollectingHandler();
        processOutput(source, handler);

        assertEquals(handler.getRows(), list(
                list(0L, "zero", 0L, "zero", 0L, "zero"),
                list(1L, "one", 1L, "one", 1L, "one"),
                list(2L, "two", 2L, "two", 2L, "two")));
    }

    private static class CollectingHandler
            extends OutputHandler
    {
        private final List<List<?>> rows = new ArrayList<>();
        private boolean finished;

        @Override
        public void processRow(List<?> values)
        {
            rows.add(values);
        }

        @Override
        public void finish()
        {
            checkState(!finished, "finish already called");
            finished = true;
        }

        public List<List<?>> getRows()
        {
            checkState(finished, "not finished");
            return rows;
        }
    }

    private static List<Object> list(Object... objects)
    {
        return ImmutableList.copyOf(objects);
    }
}
