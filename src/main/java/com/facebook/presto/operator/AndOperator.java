package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Ranges;
import com.facebook.presto.SizeOf;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.position.UncompressedPositionBlock;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import static com.facebook.presto.block.TupleStreams.getRangeFunction;
import static com.facebook.presto.block.TupleStreams.getCursorFunction;

public class AndOperator
        implements TupleStream, Iterable<TupleStream>
{
    private static final int MAX_POSITIONS_PER_BLOCK = Ints.checkedCast(BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes() / SizeOf.SIZE_OF_LONG);

    private final List<TupleStream> sources;
    private final Range range;

    public AndOperator(TupleStream... sources)
    {
        Preconditions.checkNotNull(sources, "sources is null");
        Preconditions.checkArgument(sources.length > 0, "sources is empty");

        this.sources = ImmutableList.copyOf(sources);
        this.range = Ranges.intersect(Iterables.transform(this.sources, getRangeFunction()));
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.EMPTY_TUPLE_INFO;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public Cursor cursor()
    {
        return new GenericCursor(TupleInfo.EMPTY_TUPLE_INFO, iterator());
    }

    @Override
    public Iterator<TupleStream> iterator()
    {
        final List<Cursor> cursors = ImmutableList.copyOf(Iterables.transform(sources, getCursorFunction()));

        if (!Cursors.advanceNextPosition(cursors)) {
            return Iterators.emptyIterator();
        }

        return new AndOperatorIterator(cursors);
    }

    private static class AndOperatorIterator
        extends AbstractIterator<TupleStream>
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
        protected TupleStream computeNext()
        {
            if (done) {
                endOfData();
                return null;
            }

            ImmutableList.Builder<Long> positions = ImmutableList.builder();

            int count = 0;
            while (count < MAX_POSITIONS_PER_BLOCK) {
                Cursor head = queue.remove();

                if (head.getPosition() == currentMax) {
                    // all cursors have this position in common
                    positions.add(currentMax);
                    ++count;

                    // skip past the current position
                    currentMax++;
                }

                if (!head.advanceToPosition(currentMax)) {
                    done = true;
                    break;
                }

                currentMax = Math.max(currentMax, head.getPosition());
                queue.add(head);
            }

            if (count == 0) {
                endOfData();
                return null;
            }

            return new UncompressedPositionBlock(positions.build());
        }
    }
}
