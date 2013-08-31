package com.facebook.presto.noperator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class NewTopNOperator
        implements NewOperator
{
    public static class NewTopNOperatorFactory
            implements NewOperatorFactory
    {
        private final int n;
        private final int keyChannelIndex;
        private final List<ProjectionFunction> projections;
        private final Ordering<TupleReadable> ordering;
        private final boolean partial;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewTopNOperatorFactory(
                int n,
                int keyChannelIndex,
                List<ProjectionFunction> projections,
                Ordering<TupleReadable> ordering,
                boolean partial)
        {
            this.n = n;
            this.keyChannelIndex = keyChannelIndex;
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
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");
            return new NewTopNOperator(n, keyChannelIndex, projections, ordering, partial, taskMemoryManager);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static final int MAX_INITIAL_PRIORITY_QUEUE_SIZE = 10000;
    private static final DataSize OVERHEAD_PER_TUPLE = new DataSize(100, DataSize.Unit.BYTE); // for estimating in-memory size. This is a completely arbitrary number

    private final int n;
    private final int keyChannelIndex;
    private final List<ProjectionFunction> projections;
    private final Ordering<TupleReadable> ordering;
    private final List<TupleInfo> tupleInfos;
    private final TopNMemoryManager memoryManager;
    private final boolean partial;

    private final PageBuilder pageBuilder;

    private TopNBuilder topNBuilder;
    private boolean finishing;

    private Iterator<KeyAndTuples> outputIterator;

    public NewTopNOperator(
            int n,
            int keyChannelIndex,
            List<ProjectionFunction> projections,
            Ordering<TupleReadable> ordering,
            boolean partial,
            TaskMemoryManager taskMemoryManager)
    {
        checkArgument(n > 0, "n must be greater than zero");
        this.n = n;

        checkArgument(keyChannelIndex >= 0, "keyChannelIndex must be at least zero");
        this.keyChannelIndex = keyChannelIndex;

        this.projections = ImmutableList.copyOf(checkNotNull(projections, "projections is null"));
        checkArgument(!projections.isEmpty(), "projections is empty");

        // the priority queue needs to sort in reverse order to be able to remove the least element in O(1)
        this.ordering = checkNotNull(ordering, "ordering is null").reverse();

        this.partial = partial;

        this.memoryManager = new TopNMemoryManager(checkNotNull(taskMemoryManager, "taskMemoryManager is null"));

        this.tupleInfos = toTupleInfos(projections);

        this.pageBuilder = new PageBuilder(getTupleInfos());
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
                    keyChannelIndex,
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
            KeyAndTuples next = outputIterator.next();
            for (int i = 0; i < projections.size(); i++) {
                projections.get(i).project(next.getTuples(), pageBuilder.getBlockBuilder(i));
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
        private final int keyChannelIndex;
        private final Ordering<TupleReadable> ordering;
        private final TopNMemoryManager memoryManager;
        private final PriorityQueue<KeyAndTuples> globalCandidates;

        private long memorySize;

        private TopNBuilder(int n, int keyChannelIndex, Ordering<TupleReadable> ordering, TopNMemoryManager memoryManager)
        {
            this.n = n;
            this.keyChannelIndex = keyChannelIndex;
            this.ordering = ordering;
            this.memoryManager = memoryManager;
            this.globalCandidates = new PriorityQueue<>(Math.min(n, MAX_INITIAL_PRIORITY_QUEUE_SIZE), KeyAndTuples.keyComparator(ordering));
        }

        public void processPage(Page page)
        {
            Iterable<KeyAndPosition> keyAndPositions = computePageCandidatePositions(globalCandidates, page);
            long sizeDelta = mergeWithGlobalCandidates(globalCandidates, page, keyAndPositions);
            memorySize += sizeDelta;
        }

        private long mergeWithGlobalCandidates(PriorityQueue<KeyAndTuples> globalCandidates, Page page, Iterable<KeyAndPosition> pageValueAndPositions)
        {
            long sizeDelta = 0;

            // Sort by positions so that we can advance through the values via cursors
            List<KeyAndPosition> positionSorted = Ordering.from(KeyAndPosition.positionComparator()).sortedCopy(pageValueAndPositions);

            Block[] blocks = page.getBlocks();
            BlockCursor[] cursors = new BlockCursor[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                cursors[i] = blocks[i].cursor();
            }
            for (KeyAndPosition keyAndPosition : positionSorted) {
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceToPosition(keyAndPosition.getPosition()));
                }
                if (globalCandidates.size() < n) {
                    Tuple[] tuples = getTuples(keyAndPosition, cursors);
                    for (Tuple tuple : tuples) {
                        sizeDelta += tuple.size();
                    }
                    sizeDelta += OVERHEAD_PER_TUPLE.toBytes();
                    globalCandidates.add(new KeyAndTuples(keyAndPosition.getKey(), tuples));
                }
                else if (ordering.compare(keyAndPosition.getKey(), globalCandidates.peek().getKey()) > 0) {
                    KeyAndTuples previous = globalCandidates.remove();
                    for (Tuple tuple : previous.getTuples()) {
                        sizeDelta -= tuple.size();
                    }

                    Tuple[] tuples = getTuples(keyAndPosition, cursors);
                    globalCandidates.add(new KeyAndTuples(keyAndPosition.getKey(), tuples));
                    for (Tuple tuple : tuples) {
                        sizeDelta += tuple.size();
                    }
                    sizeDelta += keyAndPosition.getKey().size();

                }
            }

            return sizeDelta;
        }

        private Iterable<KeyAndPosition> computePageCandidatePositions(PriorityQueue<KeyAndTuples> globalCandidates, Page page)
        {
            PriorityQueue<KeyAndPosition> pageCandidates = new PriorityQueue<>(Math.min(n, MAX_INITIAL_PRIORITY_QUEUE_SIZE), KeyAndPosition.keyComparator(ordering));
            KeyAndTuples smallestGlobalCandidate = globalCandidates.peek(); // This can be null if globalCandidates is empty
            BlockCursor cursor = page.getBlock(keyChannelIndex).cursor();
            while (cursor.advanceNextPosition()) {
                // Only consider value if it would be a candidate when compared against the current global candidates
                if (globalCandidates.size() < n || ordering.compare(cursor, smallestGlobalCandidate.getKey()) > 0) {
                    if (pageCandidates.size() < n) {
                        pageCandidates.add(new KeyAndPosition(cursor.getTuple(), cursor.getPosition()));
                    }
                    else if (ordering.compare(cursor, pageCandidates.peek().getKey()) > 0) {
                        pageCandidates.remove();
                        pageCandidates.add(new KeyAndPosition(cursor.getTuple(), cursor.getPosition()));
                    }
                }
            }
            return pageCandidates;
        }

        private Tuple[] getTuples(KeyAndPosition keyAndPosition, BlockCursor[] cursors)
        {
            // TODO: pre-project columns to minimize storage in global candidate set
            Tuple[] tuples = new Tuple[cursors.length];
            for (int channel = 0; channel < cursors.length; channel++) {
                // Optimization since key channel already has a materialized Tuple
                tuples[channel] = (channel == keyChannelIndex) ? keyAndPosition.getKey() : cursors[channel].getTuple();
            }
            return tuples;
        }

        private boolean isFull()
        {
            return memoryManager.canUse(memorySize);
        }

        public Iterator<KeyAndTuples> build()
        {
            ImmutableList.Builder<KeyAndTuples> minSortedGlobalCandidates = ImmutableList.builder();
            while (!globalCandidates.isEmpty()) {
                minSortedGlobalCandidates.add(globalCandidates.remove());
            }
            return minSortedGlobalCandidates.build().reverse().iterator();
        }
    }

    public static class TopNMemoryManager
    {
        private final TaskMemoryManager taskMemoryManager;
        private long currentMemoryReservation;

        public TopNMemoryManager(TaskMemoryManager taskMemoryManager)
        {
            this.taskMemoryManager = taskMemoryManager;
        }

        public boolean canUse(long memorySize)
        {
            // remove the pre-allocated memory from this size
            memorySize -= taskMemoryManager.getOperatorPreAllocatedMemory().toBytes();

            long delta = memorySize - currentMemoryReservation;
            if (delta <= 0) {
                return false;
            }

            if (!taskMemoryManager.reserveBytes(delta)) {
                return true;
            }

            // reservation worked, record the reservation
            currentMemoryReservation = Math.max(currentMemoryReservation, memorySize);
            return false;
        }

        public Object getMaxMemorySize()
        {
            return taskMemoryManager.getMaxMemorySize();
        }
    }

    private static class KeyAndPosition
    {
        private final Tuple key;
        private final int position;

        private KeyAndPosition(Tuple key, int position)
        {
            this.key = key;
            this.position = position;
        }

        public Tuple getKey()
        {
            return key;
        }

        public int getPosition()
        {
            return position;
        }

        public static Comparator<KeyAndPosition> keyComparator(final Comparator<TupleReadable> tupleReadableComparator)
        {
            return new Comparator<KeyAndPosition>()
            {
                @Override
                public int compare(KeyAndPosition o1, KeyAndPosition o2)
                {
                    return tupleReadableComparator.compare(o1.getKey(), o2.getKey());
                }
            };
        }

        public static Comparator<KeyAndPosition> positionComparator()
        {
            return new Comparator<KeyAndPosition>()
            {
                @Override
                public int compare(KeyAndPosition o1, KeyAndPosition o2)
                {
                    return Long.compare(o1.getPosition(), o2.getPosition());
                }
            };
        }
    }

    private static class KeyAndTuples
    {
        private final Tuple key;
        private final Tuple[] tuples;

        private KeyAndTuples(Tuple key, Tuple[] tuples)
        {
            this.key = key;
            this.tuples = tuples;
        }

        public Tuple getKey()
        {
            return key;
        }

        public Tuple[] getTuples()
        {
            return tuples;
        }

        public static Comparator<KeyAndTuples> keyComparator(final Comparator<TupleReadable> tupleReadableComparator)
        {
            return new Comparator<KeyAndTuples>()
            {
                @Override
                public int compare(KeyAndTuples o1, KeyAndTuples o2)
                {
                    return tupleReadableComparator.compare(o1.getKey(), o2.getKey());
                }
            };
        }
    }
}
