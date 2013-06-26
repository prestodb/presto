package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.TaskMemoryManager;
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
public class TopNOperator
        implements Operator
{
    private static final int MAX_INITIAL_PRIORITY_QUEUE_SIZE = 10000;
    private static final DataSize OVERHEAD_PER_TUPLE = new DataSize(100, DataSize.Unit.BYTE); // for estimating in-memory size. This is a completely arbitrary number

    private final Operator source;
    private final int n;
    private final int keyChannelIndex;
    private final List<ProjectionFunction> projections;
    private final Ordering<TupleReadable> ordering;
    private final List<TupleInfo> tupleInfos;
    private final TaskMemoryManager taskMemoryManager;
    private final boolean partial;

    public TopNOperator(Operator source,
            int n,
            int keyChannelIndex,
            List<ProjectionFunction> projections,
            Ordering<TupleReadable> ordering,
            boolean partial,
            TaskMemoryManager taskMemoryManager)
    {
        this.source = checkNotNull(source, "source is null");

        checkArgument(n > 0, "n must be greater than zero");
        this.n = n;

        checkArgument(keyChannelIndex >= 0, "keyChannelIndex must be at least zero");
        this.keyChannelIndex = keyChannelIndex;

        this.projections = ImmutableList.copyOf(checkNotNull(projections, "projections is null"));
        checkArgument(!projections.isEmpty(), "projections is empty");

        // the priority queue needs to sort in reverse order to be able to remove the least element in O(1)
        this.ordering = checkNotNull(ordering, "ordering is null").reverse();

        this.partial = partial;

        this.taskMemoryManager = checkNotNull(taskMemoryManager, "taskMemoryManager is null");

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
    }

    @Override
    public int getChannelCount()
    {
        return projections.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new TopNIterator(source, operatorStats, partial);
    }

    private class TopNIterator
            extends AbstractPageIterator
    {
        private final PageIterator source;
        private final PageBuilder pageBuilder;
        private final boolean partial;
        private Iterator<KeyAndTuples> outputIterator;
        private long currentMemoryReservation;

        private TopNIterator(Operator source, OperatorStats operatorStats, boolean partial)
        {
            super(tupleInfos);
            this.source = source.iterator(operatorStats);
            this.pageBuilder = new PageBuilder(getTupleInfos());
            this.partial = partial;
        }

        @Override
        protected Page computeNext()
        {
            if (outputIterator == null || !outputIterator.hasNext()) {
                outputIterator = selectTopN();
            }

            if (!outputIterator.hasNext()) {
                return endOfData();
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

        @Override
        protected void doClose()
        {
            source.close();
        }

        private Iterator<KeyAndTuples> selectTopN()
        {
            long memorySize = 0;

            PriorityQueue<KeyAndTuples> globalCandidates = new PriorityQueue<>(Math.min(n, MAX_INITIAL_PRIORITY_QUEUE_SIZE), KeyAndTuples.keyComparator(ordering));

            while (!isMaxMemoryExceeded(memorySize) && source.hasNext()) {
                Page page = source.next();
                Iterable<KeyAndPosition> keyAndPositions = computePageCandidatePositions(globalCandidates, page);
                long sizeDelta = mergeWithGlobalCandidates(globalCandidates, page, keyAndPositions);
                // reserve size for data
                memorySize = taskMemoryManager.updateOperatorReservation(memorySize, memorySize + sizeDelta);
            }

            // Only partial topN can flush early. Also, check that we are not flushing tiny bits at a time
            checkState(partial || !isMaxMemoryExceeded(memorySize), "Task exceeded max memory size of %s", taskMemoryManager.getMaxMemorySize());

            ImmutableList.Builder<KeyAndTuples> minSortedGlobalCandidates = ImmutableList.builder();
            while (!globalCandidates.isEmpty()) {
                minSortedGlobalCandidates.add(globalCandidates.remove());
            }
            return minSortedGlobalCandidates.build().reverse().iterator();
        }

        private boolean isMaxMemoryExceeded(long memorySize)
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
