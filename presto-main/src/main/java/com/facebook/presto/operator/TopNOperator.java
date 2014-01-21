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

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNOperator
        implements Operator
{
    public static class TopNOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final int n;
        private final List<ProjectionFunction> projections;
        private final Ordering<TupleReadable[]> ordering;
        private final boolean partial;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public TopNOperatorFactory(
                int operatorId,
                int n,
                List<ProjectionFunction> projections,
                Ordering<TupleReadable[]> ordering,
                boolean partial)
        {
            this.operatorId = operatorId;
            this.n = n;
            this.projections = projections;
            this.ordering = ordering;
            this.partial = partial;
            this.tupleInfos = toTupleInfos(projections);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TopNOperator.class.getSimpleName());
            return new TopNOperator(
                    operatorContext,
                    n,
                    projections,
                    ordering,
                    partial);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static final int MAX_INITIAL_PRIORITY_QUEUE_SIZE = 10000;
    private static final DataSize OVERHEAD_PER_TUPLE = new DataSize(100, DataSize.Unit.BYTE); // for estimating in-memory size. This is a completely arbitrary number

    private final OperatorContext operatorContext;
    private final int n;
    private final List<ProjectionFunction> projections;
    private final Ordering<TupleReadable[]> ordering;
    private final List<TupleInfo> tupleInfos;
    private final TopNMemoryManager memoryManager;
    private final boolean partial;

    private final PageBuilder pageBuilder;

    private TopNBuilder topNBuilder;
    private boolean finishing;

    private Iterator<TupleReadable[]> outputIterator;

    public TopNOperator(
            OperatorContext operatorContext,
            int n,
            List<ProjectionFunction> projections,
            Ordering<TupleReadable[]> ordering,
            boolean partial)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        checkArgument(n > 0, "n must be greater than zero");
        this.n = n;

        this.projections = ImmutableList.copyOf(checkNotNull(projections, "projections is null"));
        checkArgument(!projections.isEmpty(), "projections is empty");

        // the priority queue needs to sort in reverse order to be able to remove the least element in O(1)
        this.ordering = checkNotNull(ordering, "ordering is null").reverse();

        this.partial = partial;

        this.memoryManager = new TopNMemoryManager(checkNotNull(operatorContext, "operatorContext is null"));

        this.tupleInfos = toTupleInfos(projections);

        this.pageBuilder = new PageBuilder(getTupleInfos());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && topNBuilder == null && (outputIterator == null || !outputIterator.hasNext());
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputIterator == null && (topNBuilder == null || !topNBuilder.isFull());
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        if (topNBuilder == null) {
            topNBuilder = new TopNBuilder(
                    n,
                    ordering,
                    memoryManager);
        }

        checkState(!topNBuilder.isFull(), "Aggregation buffer is full");
        topNBuilder.processPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (outputIterator == null || !outputIterator.hasNext()) {
            // no data
            if (topNBuilder == null) {
                return null;
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && !topNBuilder.isFull()) {
                return null;
            }

            // Only partial aggregation can flush early. Also, check that we are not flushing tiny bits at a time
            checkState(finishing || partial, "Task exceeded max memory size of %s", memoryManager.getMaxMemorySize());

            outputIterator = topNBuilder.build();
            topNBuilder = null;
        }

        pageBuilder.reset();
        while (!pageBuilder.isFull() && outputIterator.hasNext()) {
            TupleReadable[] next = outputIterator.next();
            for (int i = 0; i < projections.size(); i++) {
                projections.get(i).project(next, pageBuilder.getBlockBuilder(i));
            }
        }

        Page page = pageBuilder.build();
        return page;
    }

    private static List<TupleInfo> toTupleInfos(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        return tupleInfos.build();
    }

    private static class TopNBuilder
    {
        private final int n;
        private final Ordering<TupleReadable[]> ordering;
        private final TopNMemoryManager memoryManager;
        private final PriorityQueue<Tuple[]> globalCandidates;

        private long memorySize;

        private TopNBuilder(int n, Ordering<TupleReadable[]> ordering, TopNMemoryManager memoryManager)
        {
            this.n = n;
            this.ordering = ordering;
            this.memoryManager = memoryManager;
            this.globalCandidates = new PriorityQueue<>(Math.min(n, MAX_INITIAL_PRIORITY_QUEUE_SIZE), ordering);
        }

        public void processPage(Page page)
        {
            long sizeDelta = mergeWithGlobalCandidates(page);
            memorySize += sizeDelta;
        }

        private long mergeWithGlobalCandidates(Page page)
        {
            long sizeDelta = 0;

            BlockCursor[] cursors = new BlockCursor[page.getChannelCount()];
            for (int i = 0; i < page.getChannelCount(); i++) {
                cursors[i] = page.getBlock(i).cursor();
            }

            for (int i = 0; i < page.getPositionCount(); i++) {
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceNextPosition());
                }

                if (globalCandidates.size() < n) {
                    sizeDelta += addRow(cursors);
                }
                else if (ordering.compare(cursors, globalCandidates.peek()) > 0) {
                    sizeDelta += addRow(cursors);
                }
            }

            return sizeDelta;
        }

        private long addRow(BlockCursor[] cursors)
        {
            long sizeDelta = 0;
            if (globalCandidates.size() >= n) {
                Tuple[] previous = globalCandidates.remove();
                for (Tuple tuple : previous) {
                    sizeDelta -= tuple.size();
                }
                sizeDelta -= OVERHEAD_PER_TUPLE.toBytes();
            }
            Tuple[] row = getValues(cursors);
            for (Tuple tuple : row) {
                sizeDelta += tuple.size();
            }
            sizeDelta += OVERHEAD_PER_TUPLE.toBytes();
            globalCandidates.add(row);
            return sizeDelta;
        }

        private Tuple[] getValues(BlockCursor[] cursors)
        {
            Tuple[] row = new Tuple[cursors.length];
            for (int i = 0; i < cursors.length; i++) {
                row[i] = cursors[i].getTuple();
            }
            return row;
        }

        private boolean isFull()
        {
            return memoryManager.canUse(memorySize);
        }

        public Iterator<TupleReadable[]> build()
        {
            ImmutableList.Builder<TupleReadable[]> minSortedGlobalCandidates = ImmutableList.builder();
            while (!globalCandidates.isEmpty()) {
                minSortedGlobalCandidates.add(globalCandidates.remove());
            }
            return minSortedGlobalCandidates.build().reverse().iterator();
        }
    }

    public static class TopNMemoryManager
    {
        private final OperatorContext operatorContext;
        private long currentMemoryReservation;

        public TopNMemoryManager(OperatorContext operatorContext)
        {
            this.operatorContext = operatorContext;
        }

        public boolean canUse(long memorySize)
        {
            // remove the pre-allocated memory from this size
            memorySize -= operatorContext.getOperatorPreAllocatedMemory().toBytes();

            long delta = memorySize - currentMemoryReservation;
            if (delta <= 0) {
                return false;
            }

            if (!operatorContext.reserveMemory(delta)) {
                return true;
            }

            // reservation worked, record the reservation
            currentMemoryReservation = Math.max(currentMemoryReservation, memorySize);
            return false;
        }

        public Object getMaxMemorySize()
        {
            return operatorContext.getMaxMemorySize();
        }
    }
}
