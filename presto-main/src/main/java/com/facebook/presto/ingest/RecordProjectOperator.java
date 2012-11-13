/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class RecordProjectOperator implements Operator
{
    private final RecordIterable source;
    private final List<? extends RecordProjection> projections;
    private final List<TupleInfo> tupleInfos;

    public RecordProjectOperator(RecordIterable source, RecordProjection... projections)
    {
        this(source, ImmutableList.copyOf(projections));
    }

    public RecordProjectOperator(RecordIterable source, Iterable<? extends RecordProjection> projections)
    {
        this.source = source;
        this.projections = ImmutableList.copyOf(projections);

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (RecordProjection projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
    }

    @Override
    public int getChannelCount()
    {
        return projections.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new RecordProjectionOperator(source.iterator(), projections);
    }

    private static class RecordProjectionOperator extends AbstractIterator<Page>
    {
        private final RecordIterator iterator;
        private final List<? extends RecordProjection> projections;

        private long outputPosition;

        public RecordProjectionOperator(RecordIterator iterator, List<? extends RecordProjection> projections)
        {
            this.iterator = iterator;
            this.projections = projections;
        }

        protected Page computeNext()
        {
            BlockBuilder[] outputs = new BlockBuilder[projections.size()];
            for (int i = 0; i < outputs.length; i++) {
                outputs[i] = new BlockBuilder(projections.get(i).getTupleInfo());

            }

            while (!isFull(outputs) && iterator.hasNext()) {
                Record record = iterator.next();
                for (int i = 0; i < projections.size(); i++) {
                    projections.get(i).project(record, outputs[i]);
                }
            }

            if (outputs[0].isEmpty()) {
                return endOfData();
            }

            Block[] blocks = new Block[projections.size()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = outputs[i].build();
            }

            Page page = new Page(blocks);
            outputPosition += blocks[0].getPositionCount();
            return page;
        }

        private boolean isFull(BlockBuilder... outputs)
        {
            for (BlockBuilder output : outputs) {
                if (output.isFull()) {
                    return true;
                }
            }
            return false;
        }
    }
}
