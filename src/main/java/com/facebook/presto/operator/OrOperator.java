package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Ranges;
import com.facebook.presto.SizeOf;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractBlockIterator;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterator;
import com.facebook.presto.block.BlockIterators;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.Cursors;
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
import static com.google.common.base.Predicates.not;

public class OrOperator
        implements TupleStream, BlockIterable<UncompressedPositionBlock>
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
    public BlockIterator<UncompressedPositionBlock> iterator()
    {
        List<Cursor> cursors = ImmutableList.copyOf(Iterables.transform(sources, getCursorFunction()));
        Cursors.advanceNextPositionNoYield(cursors);

        cursors = ImmutableList.copyOf(Iterables.filter(cursors, not(Cursors.isFinished())));
        if (cursors.isEmpty()) {
            return BlockIterators.emptyIterator();
        }

        return new OrOperatorIterator(cursors);
    }

    private static class OrOperatorIterator
            extends AbstractBlockIterator<UncompressedPositionBlock>
    {
        private final PriorityQueue<Cursor> queue;
        private long threshold = Long.MAX_VALUE;

        public OrOperatorIterator(List<Cursor> cursors)
        {
            queue = new PriorityQueue<>(cursors.size(), Cursors.orderByPosition());

            for (Cursor cursor : cursors) {
                queue.add(cursor);
                threshold = Math.min(threshold, cursor.getPosition());
            }
        }

        @Override
        protected UncompressedPositionBlock computeNext()
        {
            ImmutableList.Builder<Long> positions = ImmutableList.builder();

            int count = 0;
            AdvanceResult result = FINISHED;
            while (count < MAX_POSITIONS_PER_BLOCK && queue.size() > 0 && result != MUST_YIELD) {
                Cursor head = queue.remove();

                long position = head.getPosition();
                if (position >= threshold) {
                    positions.add(position);
                    threshold = position + 1;
                    ++count;
                }

                result = head.advanceToPosition(threshold);

                // add cursor back if it is not finished
                if (result != FINISHED) {
                    queue.add(head);
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
