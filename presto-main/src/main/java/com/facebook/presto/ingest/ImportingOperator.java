/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.nblock.Block;
import com.facebook.presto.noperator.Operator;
import com.facebook.presto.noperator.Page;
import com.facebook.presto.serde.BlocksWriter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class ImportingOperator implements Operator
{
    public static long importData(Operator source, BlockWriterFactory... blockWriterFactories)
    {
        return importData(source, ImmutableList.copyOf(blockWriterFactories));
    }

    public static long importData(Operator source, Iterable<? extends BlockWriterFactory> blockWriterFactories)
    {
        ImportingOperator importingOperator = new ImportingOperator(source, blockWriterFactories);
        long rowCount = 0;
        for (Page page : importingOperator) {
            rowCount += page.getPositionCount();
        }
        return rowCount;
    }

    private final Operator source;
    private final List<? extends BlockWriterFactory> blockWriterFactories;

    public ImportingOperator(Operator source, BlockWriterFactory... blockWriterFactories)
    {
        this(source, ImmutableList.copyOf(blockWriterFactories));
    }

    public ImportingOperator(Operator source, Iterable<? extends BlockWriterFactory> blockWriterFactories)
    {
        this.source = source;
        this.blockWriterFactories = ImmutableList.copyOf(blockWriterFactories);
    }

    @Override
    public int getChannelCount()
    {
        return source.getChannelCount();
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new AbstractIterator<Page>()
        {
            private final Iterator<Page> iterator = source.iterator();
            private List<BlocksWriter> writers;

            @Override
            protected Page computeNext()
            {
                if (!iterator.hasNext()) {
                    for (BlocksWriter writer : writers) {
                        writer.finish();
                    }
                    return endOfData();
                }

                if (writers == null) {
                    ImmutableList.Builder<BlocksWriter> builder = ImmutableList.builder();
                    for (BlockWriterFactory blockWriterFactory : blockWriterFactories) {
                        builder.add(blockWriterFactory.create());
                    }
                    writers = builder.build();
                }

                Page page = iterator.next();
                Block[] blocks = page.getBlocks();
                for (int i = 0; i < blocks.length; i++) {
                    Block block = blocks[i];
                    writers.get(i).append(block);
                }

                return page;
            }
        };
    }
}
