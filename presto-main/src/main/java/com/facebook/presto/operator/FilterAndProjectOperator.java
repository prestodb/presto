/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.ProjectionFunctions.toTupleInfos;
import static com.google.common.base.Preconditions.checkState;

public class FilterAndProjectOperator implements Operator
{
    private final Operator source;
    private final FilterFunction filter;
    private final List<ProjectionFunction> projections;
    private final List<TupleInfo> tupleInfos;

    public FilterAndProjectOperator(Operator source, FilterFunction filter, ProjectionFunction... projections)
    {
        this(source, filter, ImmutableList.copyOf(projections));
    }

    public FilterAndProjectOperator(Operator source, FilterFunction filter, List<ProjectionFunction> projections)
    {
        this.source = source;
        this.filter = filter;
        this.projections = ImmutableList.copyOf(projections);

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
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
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new FilterAndProjectIterator(source.iterator(operatorStats), filter, projections);
    }

    private static class FilterAndProjectIterator extends AbstractPageIterator
    {
        private final PageIterator pageIterator;
        private final FilterFunction filterFunction;
        private final List<ProjectionFunction> projections;

        public FilterAndProjectIterator(PageIterator pageIterator, FilterFunction filterFunction, List<ProjectionFunction> projections)
        {
            super(toTupleInfos(projections));
            this.pageIterator = pageIterator;
            this.filterFunction = filterFunction;
            this.projections = projections;
        }

        protected Page computeNext()
        {
            PageBuilder pageBuilder = new PageBuilder(getTupleInfos());
            while (!pageBuilder.isFull() && pageIterator.hasNext()) {
                Page page = pageIterator.next();
                Block[] blocks = page.getBlocks();
                filterAndProjectRowOriented(blocks, pageBuilder);
            }

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }

            Page page = pageBuilder.build();
            return page;
        }

        @Override
        protected void doClose()
        {
            pageIterator.close();
        }

        private void filterAndProjectRowOriented(Block[] blocks, PageBuilder pageBuilder)
        {
            int rows = blocks[0].getPositionCount();

            BlockCursor[] cursors = new BlockCursor[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                cursors[i] = blocks[i].cursor();
            }

            for (int position = 0; position < rows; position++) {
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceNextPosition());
                }

                if (filterFunction.filter(cursors)) {
                    pageBuilder.declarePosition();
                    for (int i = 0; i < projections.size(); i++) {
                        // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                        projections.get(i).project(cursors, pageBuilder.getBlockBuilder(i));
                    }
                }
            }

            for (BlockCursor cursor : cursors) {
                checkState(!cursor.advanceNextPosition());
            }
        }
    }
}
