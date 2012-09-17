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

import static com.facebook.presto.block.TupleStreams.getCursorFunction;
import static com.facebook.presto.block.TupleStreams.getRangeFunction;
import static com.google.common.base.Predicates.not;

public class OrOperator
        implements TupleStream, Iterable<TupleStream>
{
    private static final int MAX_POSITIONS_PER_BLOCK = Ints.checkedCast(BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes() / SizeOf.SIZE_OF_LONG);

    private final List<TupleStream> sources;
    private final Range range;

    public OrOperator(TupleStream... sources)
    {
        Preconditions.checkNotNull(sources, "sources is null");
        Preconditions.checkArgument(sources.length > 0, "sources is empty");

        this.sources = ImmutableList.copyOf(sources);
        this.range = Ranges.merge(Iterables.transform(this.sources, getRangeFunction()));
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
    public Cursor cursor()
    {
        return new GenericCursor(TupleInfo.EMPTY, iterator());
    }

    @Override
    public Iterator<TupleStream> iterator()
    {
        List<Cursor> cursors = ImmutableList.copyOf(Iterables.transform(sources, getCursorFunction()));
        Cursors.advanceNextPosition(cursors);

        cursors = ImmutableList.copyOf(Iterables.filter(cursors, not(Cursors.isFinished())));
        if (cursors.isEmpty()) {
            return Iterators.emptyIterator();
        }

        return new OrOperatorIterator(cursors);
    }

    private static class OrOperatorIterator
        extends AbstractIterator<TupleStream>
    {
        private final PriorityQueue<Cursor> queue;
        private long threshold = Long.MAX_VALUE;

        public OrOperatorIterator(List<Cursor> cursors)
        {
            queue = new PriorityQueue<>(cursors.size(), Cursors.orderByPosition());

            for (Cursor cursor : cursors) {
                queue.add(cursor);
                threshold =  Math.min(threshold, cursor.getPosition());
            }
        }

        @Override
        protected TupleStream computeNext()
        {
            ImmutableList.Builder<Long> positions = ImmutableList.builder();

            int count = 0;
            while (count < MAX_POSITIONS_PER_BLOCK && queue.size() > 0) {
                Cursor head = queue.remove();

                long position = head.getPosition();
                if (position >= threshold) {
                    positions.add(position);
                    threshold = position + 1;
                    ++count;
                }

                if (head.advanceToPosition(threshold)) {
                    queue.add(head);
                }
            }

            if (count == 0) {
                endOfData();
                return null;
            }

            return new UncompressedPositionBlock(positions.build());
        }
    }
}
