package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Ranges;
import com.facebook.presto.SizeOf;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.facebook.presto.block.YieldingIterators;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.position.UncompressedPositionBlock;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.PriorityQueue;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.TupleStreams.getCursorFunction;
import static com.facebook.presto.block.TupleStreams.getRangeFunction;

public class AndOperator
        implements TupleStream, YieldingIterable<UncompressedPositionBlock>
{
    private static final int MAX_POSITIONS_PER_BLOCK = Ints.checkedCast(BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes() / SizeOf.SIZE_OF_LONG);

    private final List<TupleStream> sources;
    private final Range range;

    public AndOperator(List<TupleStream> sources)
    {
        Preconditions.checkNotNull(sources, "sources is null");
        Preconditions.checkArgument(!sources.isEmpty(), "sources is empty");

        this.sources = ImmutableList.copyOf(sources);
        this.range = Ranges.intersect(Iterables.transform(this.sources, getRangeFunction()));

    }

    public AndOperator(TupleStream... sources)
    {
        this(ImmutableList.copyOf(sources));
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.EMPTY;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new GenericCursor(session, TupleInfo.EMPTY, iterator(session));
    }

    @Override
    public YieldingIterator<UncompressedPositionBlock> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");

        final List<Cursor> cursors = ImmutableList.copyOf(Iterables.transform(sources, getCursorFunction(session)));

        if (!Cursors.advanceNextPositionNoYield(cursors)) {
            return YieldingIterators.emptyIterator();
        }

        return new AndOperatorIterator(cursors);
    }

    private static class AndOperatorIterator
            extends AbstractYieldingIterator<UncompressedPositionBlock>
    {
        private final PriorityQueue<Cursor> queue;
        private boolean done = false;
        private long currentMax = -1;

        public AndOperatorIterator(List<Cursor> cursors)
        {
            queue = new PriorityQueue<>(cursors.size(), Cursors.orderByPosition());

            for (Cursor cursor : cursors) {
                queue.add(cursor);
                currentMax = Math.max(currentMax, cursor.getPosition());
            }
        }

        @Override
        protected UncompressedPositionBlock computeNext()
        {
            if (done) {
                endOfData();
                return null;
            }

            ImmutableList.Builder<Long> positions = ImmutableList.builder();

            int count = 0;
            AdvanceResult result = FINISHED;
            while (count < MAX_POSITIONS_PER_BLOCK) {
                Cursor head = queue.remove();

                long initialPosition = head.getPosition();
                if (initialPosition == currentMax) {
                    // all cursors have this position in common
                    positions.add(currentMax);
                    ++count;

                    // skip past the current position
                    currentMax++;
                }

                result = head.advanceToPosition(currentMax);
                if (result == FINISHED) {
                    done = true;
                    break;
                }

                long endPosition = head.getPosition();
                currentMax = Math.max(currentMax, endPosition);
                queue.add(head);

                if (result == MUST_YIELD && initialPosition == endPosition) {
                    break;
                }
            }

            if (count == 0) {
                switch (result) {
                    case SUCCESS:
                        throw new IllegalStateException("No positions produced and input was not finished");
                    case MUST_YIELD:
                        return setMustYield();
                    case FINISHED:
                        return endOfData();
                }
            }

            return new UncompressedPositionBlock(positions.build());
        }
    }
}
