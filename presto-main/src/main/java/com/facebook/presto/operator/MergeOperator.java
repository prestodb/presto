package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Assumes all of the provided sources are already position aligned
 */
public class MergeOperator
        implements TupleStream
{
    private final List<? extends TupleStream> sources;
    private final TupleInfo tupleInfo;

    public MergeOperator(TupleStream... sources)
    {
        this(ImmutableList.copyOf(sources));
    }

    public MergeOperator(Iterable<? extends TupleStream> sources)
    {
        Preconditions.checkNotNull(sources, "sources is null");
        this.sources = ImmutableList.copyOf(sources);
        Preconditions.checkArgument(!this.sources.isEmpty(), "must provide at least one source");

        // Build combined tuple info
        ImmutableList.Builder<TupleInfo.Type> types = ImmutableList.builder();
        for (TupleStream source : sources) {
            types.addAll(source.getTupleInfo().getTypes());
        }
        this.tupleInfo = new TupleInfo(types.build());
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
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
        ImmutableList.Builder<Cursor> builder = ImmutableList.builder();
        for (TupleStream tupleStream : sources) {
            builder.add(tupleStream.cursor(session));
        }
        return new MergeCursor(builder.build());
    }
}
