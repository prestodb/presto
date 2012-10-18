package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

public class ApplyPredicateOperator
        implements TupleStream
{
    private final TupleStream source;
    private final Predicate<Cursor> predicate;

    public ApplyPredicateOperator(TupleStream source, Predicate<Cursor> predicate)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(predicate, "predicate is null");
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return source.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new FilteredCursor(predicate, source.cursor(session));
    }
}
