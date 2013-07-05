package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.metadata.ColumnFileHandle;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.NativeSplit;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableWriterOperator
        implements SourceOperator, OutputProducingOperator<TableWriterResult>
{
    private final LocalStorageManager storageManager;
    private final String nodeIdentifier;
    private final Operator sourceOperator;
    private final List<ColumnHandle> columnHandles;

    private final AtomicBoolean used = new AtomicBoolean();

    private final AtomicReference<NativeSplit> input = new AtomicReference<>();

    private final AtomicReference<Set<TableWriterResult>> output = new AtomicReference<Set<TableWriterResult>>(ImmutableSet.<TableWriterResult>of());

    public TableWriterOperator(LocalStorageManager storageManager,
            String nodeIdentifier,
            List<ColumnHandle> columnHandles,
            Operator sourceOperator)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");
        this.sourceOperator = checkNotNull(sourceOperator, "sourceOperator is null");

        checkState(sourceOperator.getChannelCount() == columnHandles.size(), "channel count does not match columnHandles list");
    }

    @Override
    public void addSplit(Split split)
    {
        checkNotNull(split, "split is null");
        checkState(split instanceof NativeSplit, "Non-native split added!");
        checkState(input.get() == null, "Shard Id %s was already set!", input.get());
        input.set((NativeSplit) split);
    }

    @Override
    public void noMoreSplits()
    {
        checkState(input.get() != null, "No shard id was set!");
    }

    @Override
    public Set<TableWriterResult> getOutput()
    {
        return output.get();
    }

    @Override
    public int getChannelCount()
    {
        return 1;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return ImmutableList.of(SINGLE_LONG);
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        checkNotNull(operatorStats, "operatorStats is null");

        checkState(!used.getAndSet(true), "TableWriteOperator can be used only once");
        checkState(input.get() != null, "No shard id was set!");

        try {
            ColumnFileHandle columnFileHandle = storageManager.createStagingFileHandles(input.get().getShardId(), columnHandles);
            return new TableWriteIterator(operatorStats, sourceOperator.iterator(operatorStats), columnFileHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void commitFileHandle(ColumnFileHandle columnFileHandle)
    {
        try {
            storageManager.commit(columnFileHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    class TableWriteIterator
            extends AbstractPageIterator
    {
        private final OperatorStats operatorStats;
        private final ColumnFileHandle fileHandle;
        private final PageIterator sourceIterator;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TableWriteIterator(OperatorStats operatorStats, PageIterator sourceIterator, ColumnFileHandle fileHandle)
        {
            super(sourceIterator.getTupleInfos());
            this.operatorStats = operatorStats;
            this.sourceIterator = sourceIterator;
            this.fileHandle = fileHandle;
        }

        @Override
        protected void doClose()
        {
            if (!closed.getAndSet(true)) {
                checkState(operatorStats.isDone() || !sourceIterator.hasNext(), "Writer was closed while source iterator still has data and the operator is not done!");
                commitFileHandle(fileHandle);
                sourceIterator.close();
                output.set(ImmutableSet.of(new TableWriterResult(input.get().getShardId(), nodeIdentifier)));
            }
        }

        @Override
        protected Page computeNext()
        {
            if (operatorStats.isDone() || !sourceIterator.hasNext()) {
                return endOfData();
            }

            int rowCount = fileHandle.append(sourceIterator.next());

            Block block = new BlockBuilder(SINGLE_LONG).append(rowCount).build();
            return new Page(block);
        }
    }
}
