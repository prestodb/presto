/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
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

import java.io.Closeable;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;

public abstract class AbstractScanFilterAndProjectOperator
        implements SourceOperator, Closeable
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
    private Operator operator;

    private boolean finishing;

    private long completedBytes;

    protected AbstractScanFilterAndProjectOperator(
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

        Operator dataStream = dataStreamProvider.createNewDataStream(operatorContext, split, columns);
        if (dataStream instanceof RecordProjectOperator) {
            cursor = ((RecordProjectOperator) dataStream).getCursor();
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
        close();
    }

    public void close()
    {
        if (operator != null) {
            operator.finish();
        }
        else if (cursor != null) {
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
        }
        else {
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
