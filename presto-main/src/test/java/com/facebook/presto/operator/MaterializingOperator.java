package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MaterializingOperator
        implements Operator
{
    public static class MaterializingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<TupleInfo> sourceTupleInfos;
        private boolean closed;

        public MaterializingOperatorFactory(int operatorId, List<TupleInfo> sourceTupleInfos)
        {
            this.operatorId = operatorId;
            this.sourceTupleInfos = sourceTupleInfos;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return ImmutableList.of();
        }

        @Override
        public MaterializingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MaterializingOperator.class.getSimpleName());
            return new MaterializingOperator(operatorContext, sourceTupleInfos);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final MaterializedResult.Builder resultBuilder;
    private boolean finished;

    public MaterializingOperator(OperatorContext operatorContext, List<TupleInfo> sourceTupleInfos)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TupleInfo sourceTupleInfo : checkNotNull(sourceTupleInfos, "sourceTupleInfos is null")) {
            types.addAll(sourceTupleInfo.getTypes());
        }
        resultBuilder = MaterializedResult.resultBuilder(new TupleInfo(types.build()));
    }

    public MaterializedResult getMaterializedResult()
    {
        return resultBuilder.build();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finished, "operator finished");

        resultBuilder.page(page);
        operatorContext.recordGeneratedOutput(page.getDataSize(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
