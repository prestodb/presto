package com.facebook.presto.noperator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MaterializingOperator
        implements NewOperator
{
    public static class MaterializingOperatorFactory
            implements NewOperatorFactory
    {
        private final List<TupleInfo> sourceTupleInfos;
        private boolean closed;

        public MaterializingOperatorFactory(List<TupleInfo> sourceTupleInfos)
        {
            this.sourceTupleInfos = sourceTupleInfos;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return ImmutableList.of();
        }

        @Override
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");
            return new MaterializingOperator(sourceTupleInfos);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final ImmutableList.Builder<Tuple> tuples = ImmutableList.builder();
    private final TupleInfo outputTupleInfo;
    private boolean finished;

    public MaterializingOperator(List<TupleInfo> sourceTupleInfos)
    {

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TupleInfo sourceTupleInfo : sourceTupleInfos) {
            types.addAll(sourceTupleInfo.getTypes());
        }
        outputTupleInfo = new TupleInfo(types.build());
    }

    public MaterializedResult getMaterializedResult()
    {
        return new MaterializedResult(tuples.build(), outputTupleInfo);
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
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finished, "operator finished");


        List<TupleInfo> tupleInfos = new ArrayList<>();
        List<BlockCursor> cursors = new ArrayList<>();
        for (Block block : page.getBlocks()) {
            cursors.add(block.cursor());
            tupleInfos.add(block.getTupleInfo());
        }

        while (true) {
            TupleInfo.Builder builder = outputTupleInfo.builder();

            boolean hasResults = false;
            for (BlockCursor cursor : cursors) {
                if (cursor.advanceNextPosition()) {
                    builder.append(cursor.getTuple());
                    hasResults = true;
                }
                else {
                    checkState(!hasResults, "unaligned cursors");
                }
            }
            if (!hasResults) {
                return;
            }
            tuples.add(builder.build());
        }
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
