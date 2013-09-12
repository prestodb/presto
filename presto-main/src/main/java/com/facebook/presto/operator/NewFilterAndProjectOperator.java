package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NewFilterAndProjectOperator
        extends NewAbstractFilterAndProjectOperator
{
    public static class NewFilterAndProjectOperatorFactory
            implements NewOperatorFactory
    {
        private final int operatorId;
        private final FilterFunction filterFunction;
        private final List<ProjectionFunction> projections;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewFilterAndProjectOperatorFactory(int operatorId, FilterFunction filterFunction, ProjectionFunction... projections)
        {
            this(operatorId, filterFunction, ImmutableList.copyOf(checkNotNull(projections, "projections is null")));
        }

        public NewFilterAndProjectOperatorFactory(int operatorId, FilterFunction filterFunction, List<ProjectionFunction> projections)
        {
            this.operatorId = operatorId;
            this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
            this.projections = ImmutableList.copyOf(projections);
            this.tupleInfos = toTupleInfos(checkNotNull(projections, "projections is null"));
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public NewOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, NewFilterAndProjectOperator.class.getSimpleName());
            return new NewFilterAndProjectOperator(operatorContext, filterFunction, projections);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final FilterFunction filterFunction;
    private final List<ProjectionFunction> projections;

    public NewFilterAndProjectOperator(OperatorContext operatorContext, FilterFunction filterFunction, ProjectionFunction... projections)
    {
        this(operatorContext, filterFunction, ImmutableList.copyOf(checkNotNull(projections, "projections is null")));
    }

    public NewFilterAndProjectOperator(OperatorContext operatorContext, FilterFunction filterFunction, List<ProjectionFunction> projections)
    {
        super(operatorContext, toTupleInfos(checkNotNull(projections, "projections is null")));
        this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
        this.projections = ImmutableList.copyOf(projections);
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

    private static List<TupleInfo> toTupleInfos(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        return tupleInfos.build();
    }
}
