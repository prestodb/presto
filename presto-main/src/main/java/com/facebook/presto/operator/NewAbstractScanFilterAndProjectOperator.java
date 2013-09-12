package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;

public abstract class NewAbstractScanFilterAndProjectOperator
        implements NewSourceOperator
{
    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final DataStreamProvider dataStreamProvider;
    private final List<TupleInfo> tupleInfos;
    private final List<ColumnHandle> columns;
    private final PageBuilder pageBuilder;

    @GuardedBy("this")
    private RecordCursor cursor;

    @GuardedBy("this")
    private NewOperator operator;

    private boolean finishing;

    private long completedBytes;

    protected NewAbstractScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            DataStreamProvider dataStreamProvider,
            Iterable<ColumnHandle> columns,
            Iterable<TupleInfo> tupleInfos)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.planNodeId = checkNotNull(sourceId, "sourceId is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.tupleInfos = ImmutableList.copyOf(checkNotNull(tupleInfos, "tupleInfos is null"));
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));

        this.pageBuilder = new PageBuilder(getTupleInfos());
    }

    protected abstract void filterAndProjectRowOriented(Block[] blocks, PageBuilder pageBuilder);

    protected abstract int filterAndProjectRowOriented(RecordCursor cursor, PageBuilder pageBuilder);

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public synchronized void addSplit(final Split split)
    {
        checkNotNull(split, "split is null");
        checkState(cursor == null && operator == null, "split already set");

        NewOperator dataStream = dataStreamProvider.createNewDataStream(operatorContext, split, columns);
        if (dataStream instanceof NewRecordProjectOperator) {
            cursor = ((NewRecordProjectOperator) dataStream).getCursor();
        }
        else {
            operator = dataStream;
        }

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(splitInfo));
        }
    }

    @Override
    public synchronized void noMoreSplits()
    {
        if (cursor == null && operator == null) {
            finishing = true;
        }
    }

    @Override
    public final List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public final void finish()
    {
        if (operator != null) {
            operator.finish();
        } else {
            cursor.close();
        }
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        if (operator != null && operator.isFinished()) {
            finishing = true;
        }

        return finishing && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (operator != null) {
            return operator.isBlocked();
        } else {
            return NOT_BLOCKED;
        }
    }

    @Override
    public final boolean needsInput()
    {
        return false;
    }

    @Override
    public final void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!finishing) {
            if (cursor != null) {
                int rowsProcessed = filterAndProjectRowOriented(cursor, pageBuilder);
                long bytesProcessed = cursor.getCompletedBytes() - completedBytes;
                operatorContext.recordGeneratedInput(new DataSize(bytesProcessed, BYTE), rowsProcessed);
                completedBytes += bytesProcessed;

                if (rowsProcessed == 0) {
                    finishing = true;
                }
            }
            else {
                Page output = operator.getOutput();
                if (output != null) {
                    filterAndProjectRowOriented(output.getBlocks(), pageBuilder);
                }
            }
        }

        // only return a full page is buffer is full or we are finishing
        if (pageBuilder.isEmpty() || (!finishing && !pageBuilder.isFull())) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }
}
