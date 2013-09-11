/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class SamplingOperator
        extends AbstractSamplingOperator
{
    private final double samplePercentageValue;
    private final List<ProjectionFunction> projections;

    public SamplingOperator(Operator source, double samplePercentageValue, ProjectionFunction... projections)
    {
        this(source, samplePercentageValue, ImmutableList.copyOf(projections));
    }

    public SamplingOperator(Operator source, double samplePercentageValue, List<ProjectionFunction> projections)
    {
        super(toTupleInfos(projections), source);
        this.samplePercentageValue = samplePercentageValue;
        this.projections = ImmutableList.copyOf(projections);
    }

    private static ImmutableList<TupleInfo> toTupleInfos(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        return tupleInfos.build();
    }

    @Override
    protected PageIterator iterator(PageIterator source, OperatorStats operatorStats)
    {
        return new SamplingIterator(source, samplePercentageValue, projections, operatorStats);
    }

    public static class SamplingIterator
            extends AbstractSamplingIterator
    {
        private final double samplePercentageValue;
        private final List<ProjectionFunction> projections;
        private final OperatorStats operatorStats;
        private long currentCompletedSize;

        public SamplingIterator(PageIterator pageIterator, double samplePercentageValue, List<ProjectionFunction> projections, OperatorStats operatorStats)
        {
            super(toTupleInfos(projections), pageIterator, operatorStats);

            Preconditions.checkArgument(samplePercentageValue >= 0.0 && samplePercentageValue <= 100.0, "sampling percentage must be between 0 and 100");

            this.samplePercentageValue = samplePercentageValue;
            this.projections = projections;
            this.operatorStats = operatorStats;
        }

        protected void samplingRowOriented(Block[] blocks, PageBuilder pageBuilder)
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

                if (Math.random() <= samplePercentageValue / 100.0) {
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

        @Override
        protected void samplingRowOriented(RecordCursor cursor, PageBuilder pageBuilder)
        {
            long completedPositions = 0;
            while (!pageBuilder.isFull() && cursor.advanceNextPosition()) {
                completedPositions++;
                // every 16K rows check if the query is finished
                if (shouldCheckDoneFlag(completedPositions) && operatorStats.isDone()) {
                    break;
                }
                if (Math.random() <= samplePercentageValue / 100.0) {
                    pageBuilder.declarePosition();
                    for (int i = 0; i < projections.size(); i++) {
                        // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                        projections.get(i).project(cursor, pageBuilder.getBlockBuilder(i));
                    }
                }
            }

            long completedDataSize = cursor.getCompletedBytes();
            if (completedDataSize > currentCompletedSize) {
                operatorStats.addCompletedDataSize(completedDataSize - currentCompletedSize);
                currentCompletedSize = completedDataSize;
            }
            operatorStats.addCompletedPositions(completedPositions);
        }
    }
}
