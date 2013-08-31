package com.facebook.presto.noperator;

import com.facebook.presto.block.Block;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class NewAbstractFilterAndProjectOperator
        implements NewOperator
{
    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;

    private final PageBuilder pageBuilder;
    private boolean finishing;

    public NewAbstractFilterAndProjectOperator(OperatorContext operatorContext, Iterable<TupleInfo> tupleInfos)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.tupleInfos = ImmutableList.copyOf(checkNotNull(tupleInfos, "tupleInfos is null"));
        this.pageBuilder = new PageBuilder(getTupleInfos());
    }

    protected abstract void filterAndProjectRowOriented(Block[] blocks, PageBuilder pageBuilder);

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public final List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public final void finish()
    {
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public final boolean needsInput()
    {
        return !finishing && !pageBuilder.isFull();
    }

    @Override
    public final void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        checkState(!pageBuilder.isFull(), "Page buffer is full");

        Block[] blocks = page.getBlocks();
        filterAndProjectRowOriented(blocks, pageBuilder);
    }

    @Override
    public final Page getOutput()
    {
        if (needsInput() || pageBuilder.isEmpty()) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }
}
