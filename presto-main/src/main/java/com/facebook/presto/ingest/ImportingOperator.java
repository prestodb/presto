/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.block.Block;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.serde.BlocksFileWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.block.BlockUtils.toTupleIterable;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportingOperator implements Operator
{
    public static long importData(Operator source, BlocksFileWriter... fileWriters)
    {
        return importData(source, ImmutableList.copyOf(fileWriters));
    }

    public static long importData(Operator source, Iterable<? extends BlocksFileWriter> fileWriters)
    {
        ImportingOperator importingOperator = new ImportingOperator(source, fileWriters);
        long rowCount = 0;
        for (Page page : importingOperator) {
            rowCount += page.getPositionCount();
        }
        return rowCount;
    }

    private final Operator source;
    private final List<? extends BlocksFileWriter> fileWriters;
    private boolean used;

    public ImportingOperator(Operator source, BlocksFileWriter... fileWriters)
    {
        this(source, ImmutableList.copyOf(checkNotNull(fileWriters, "fileWriters is null")));
    }

    public ImportingOperator(Operator source, Iterable<? extends BlocksFileWriter> fileWriters)
    {
        checkNotNull(source, "source is null");
        checkNotNull(fileWriters, "fileWriters is null");
        this.source = source;
        this.fileWriters = ImmutableList.copyOf(fileWriters);
    }

    @Override
    public int getChannelCount()
    {
        return source.getChannelCount();
    }

    @Override
    public Iterator<Page> iterator()
    {
        Preconditions.checkState(!used, "Import operator can only be used once");
        used = true;

        return new AbstractIterator<Page>()
        {
            private final Iterator<Page> iterator = source.iterator();

            @Override
            protected Page computeNext()
            {
                if (!iterator.hasNext()) {
                    close();
                    return endOfData();
                }

                Page page = iterator.next();
                Block[] blocks = page.getBlocks();
                for (int i = 0; i < blocks.length; i++) {
                    Block block = blocks[i];
                    fileWriters.get(i).append(toTupleIterable(block));
                }

                return page;
            }

            public void close()
            {
                for (BlocksFileWriter fileWriter : fileWriters) {
                    fileWriter.close();
                }
                endOfData();
            }
        };
    }
}
