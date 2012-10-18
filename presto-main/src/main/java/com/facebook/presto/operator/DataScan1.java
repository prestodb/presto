package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.position.PositionsBlock;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class DataScan1
        implements TupleStream, YieldingIterable<PositionsBlock>
{
    private static final int RANGES_PER_BLOCK = 100;

    private final TupleStream source;
    private final Predicate<Cursor> predicate;

    public DataScan1(TupleStream source, Predicate<Cursor> predicate)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(predicate, "predicate is null");
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.EMPTY;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    public YieldingIterator<PositionsBlock> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new AbstractYieldingIterator<PositionsBlock>()
        {
            Cursor cursor = source.cursor(new QuerySession());

            @Override
            protected PositionsBlock computeNext()
            {
                int rangesCount = 0;
                ImmutableList.Builder<Range> ranges = ImmutableList.builder();
                while (rangesCount < RANGES_PER_BLOCK) {
                    AdvanceResult result = cursor.advanceNextValue();
                    if (result != SUCCESS) {
                        if (rangesCount != 0) {
                            break;
                        }
                        if (result == MUST_YIELD) {
                            return setMustYield();
                        }
                        else if (result == FINISHED) {
                            return endOfData();
                        }
                    }
                    if (predicate.apply(cursor)) {
                        ranges.add(new Range(cursor.getPosition(), cursor.getCurrentValueEndPosition()));
                        rangesCount++;
                    }
                }
                return new PositionsBlock(ranges.build());
            }
        };
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new GenericCursor(session, TupleInfo.EMPTY, iterator(session));
    }
}
