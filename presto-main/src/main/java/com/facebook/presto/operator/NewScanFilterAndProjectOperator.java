package com.facebook.presto.noperator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NewScanFilterAndProjectOperator
        extends NewAbstractScanFilterAndProjectOperator
{
    public static class NewScanFilterAndProjectOperatorFactory
            implements NewSourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final DataStreamProvider dataStreamProvider;
        private final List<ColumnHandle> columns;
        private final FilterFunction filterFunction;
        private final List<ProjectionFunction> projections;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                DataStreamProvider dataStreamProvider,
                Iterable<ColumnHandle> columns,
                FilterFunction filterFunction,
                ProjectionFunction... projections)
        {
            this(operatorId, sourceId, dataStreamProvider, columns, filterFunction, ImmutableList.copyOf(checkNotNull(projections, "projections is null")));
        }

        public NewScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                DataStreamProvider dataStreamProvider,
                Iterable<ColumnHandle> columns,
                FilterFunction filterFunction,
                Iterable<ProjectionFunction> projections)
        {
            this.operatorId = operatorId;
            this.sourceId = checkNotNull(sourceId, "sourceId is null");
            this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
            this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
            this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
            this.projections = ImmutableList.copyOf(checkNotNull(projections, "projections is null"));
            this.tupleInfos = toTupleInfos(this.projections);
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public NewSourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, NewFilterAndProjectOperator.class.getSimpleName());
            return new NewScanFilterAndProjectOperator(operatorContext, sourceId, dataStreamProvider, columns, filterFunction, projections);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final FilterFunction filterFunction;
    private final List<ProjectionFunction> projections;

    public NewScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            DataStreamProvider dataStreamProvider,
            Iterable<ColumnHandle> columns,
            FilterFunction filterFunction,
            ProjectionFunction... projections)
    {
        this(operatorContext,
                sourceId,
                dataStreamProvider,
                columns,
                filterFunction,
                ImmutableList.copyOf(checkNotNull(projections, "projections is null")));
    }

    public NewScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            DataStreamProvider dataStreamProvider,
            Iterable<ColumnHandle> columns,
            FilterFunction filterFunction,
            Iterable<ProjectionFunction> projections)
    {
        super(operatorContext,
                sourceId,
                dataStreamProvider,
                columns,
                toTupleInfos(ImmutableList.copyOf(checkNotNull(projections, "projections is null"))));
        this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
        this.projections = ImmutableList.copyOf(checkNotNull(projections, "projections is null"));
    }

    protected void filterAndProjectRowOriented(Block[] blocks, PageBuilder pageBuilder)
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

    @Override
    protected int filterAndProjectRowOriented(RecordCursor cursor, PageBuilder pageBuilder)
    {
        int completedPositions = 0;
        for (; completedPositions < 16384; completedPositions++) {
            if (pageBuilder.isFull()) {
                break;
            }

            if (!cursor.advanceNextPosition()) {
                break;
            }

            if (filterFunction.filter(cursor)) {
                pageBuilder.declarePosition();
                for (int channel = 0; channel < projections.size(); channel++) {
                    // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                    projections.get(channel).project(cursor, pageBuilder.getBlockBuilder(channel));
                }
            }
        }
        return completedPositions;
    }

    private static List<TupleInfo> toTupleInfos(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        return tupleInfos.build();
    }
}
