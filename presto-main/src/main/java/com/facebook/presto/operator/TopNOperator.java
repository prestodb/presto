package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

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
    private final Operator source;
    private final int n;
    private final int keyChannelIndex;
    private final List<ProjectionFunction> projections;
    private final Ordering<TupleReadable> ordering;
    private final List<TupleInfo> tupleInfos;

    public TopNOperator(Operator source, int n, int keyChannelIndex, List<ProjectionFunction> projections, Ordering<TupleReadable> ordering)
    {
        checkNotNull(source, "source is null");
        checkArgument(n > 0, "n must be greater than zero");
        checkArgument(keyChannelIndex >= 0, "keyChannelIndex must be at least zero");
        checkNotNull(projections, "projections is null");
        checkArgument(!projections.isEmpty(), "projections is empty");
        checkNotNull(ordering, "ordering is null");
        this.source = source;
        this.n = n;
        this.keyChannelIndex = keyChannelIndex;
        this.projections = ImmutableList.copyOf(projections);
        this.ordering = ordering;

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
    }

    public TopNOperator(Operator source, int n, int keyChannelIndex, List<ProjectionFunction> projections)
    {
        this(source, n, keyChannelIndex, projections, Ordering.from(FieldOrderedTupleComparator.INSTANCE));
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
        return new TopNIterator(source, operatorStats);
    }

    private class TopNIterator
            extends AbstractPageIterator
    {
        private Iterator<KeyAndTuples> outputIterator;
        private PageIterator source;

        private TopNIterator(Operator source, OperatorStats operatorStats)
        {
            super(source.getTupleInfos());
            this.source = source.iterator(operatorStats);
        }

        @Override
        protected Page computeNext()
        {
            if (outputIterator == null) {
                outputIterator = selectTopN(source);
            }

            if (!outputIterator.hasNext()) {
                return endOfData();
            }

            BlockBuilder[] outputs = new BlockBuilder[projections.size()];
            for (int i = 0; i < outputs.length; i++) {
                outputs[i] = new BlockBuilder(projections.get(i).getTupleInfo());
            }

            while (!isFull(outputs) && outputIterator.hasNext()) {
                KeyAndTuples next = outputIterator.next();
                for (int i = 0; i < projections.size(); i++) {
                    projections.get(i).project(next.getTuples(), outputs[i]);
                }
            }

            Block[] blocks = new Block[projections.size()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = outputs[i].build();
            }

            Page page = new Page(blocks);
            return page;
        }

        @Override
        protected void doClose()
        {
            source.close();
        }

        private Iterator<KeyAndTuples> selectTopN(PageIterator iterator)
        {
            PriorityQueue<KeyAndTuples> globalCandidates = new PriorityQueue<>(n, KeyAndTuples.keyComparator(ordering));
            try (PageIterator pageIterator = iterator) {
                while (pageIterator.hasNext()) {
                    Page page = pageIterator.next();
                    Iterable<KeyAndPosition> keyAndPositions = computePageCandidatePositions(globalCandidates, page);
                    mergeWithGlobalCandidates(globalCandidates, page, keyAndPositions);
                }
            }
            ImmutableList.Builder<KeyAndTuples> minSortedGlobalCandidates = ImmutableList.builder();
            while (!globalCandidates.isEmpty()) {
                minSortedGlobalCandidates.add(globalCandidates.remove());
            }
            return minSortedGlobalCandidates.build().reverse().iterator();
        }

        private Iterable<KeyAndPosition> computePageCandidatePositions(PriorityQueue<KeyAndTuples> globalCandidates, Page page)
        {
            PriorityQueue<KeyAndPosition> pageCandidates = new PriorityQueue<>(n, KeyAndPosition.keyComparator(ordering));
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

        private void mergeWithGlobalCandidates(PriorityQueue<KeyAndTuples> globalCandidates, Page page, Iterable<KeyAndPosition> pageValueAndPositions)
        {
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
                    globalCandidates.add(new KeyAndTuples(keyAndPosition.getKey(), getTuples(keyAndPosition, cursors)));
                }
                else if (ordering.compare(keyAndPosition.getKey(), globalCandidates.peek().getKey()) > 0) {
                    globalCandidates.remove();
                    globalCandidates.add(new KeyAndTuples(keyAndPosition.getKey(), getTuples(keyAndPosition, cursors)));
                }
            }
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

        private boolean isFull(BlockBuilder... outputs)
        {
            for (BlockBuilder output : outputs) {
                if (output.isFull()) {
                    return true;
                }
            }
            return false;
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
