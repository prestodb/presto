package com.facebook.presto.operator.inlined;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Adapter to allow fetching partial inlined operation results
 */
public class PartialInlinedOperatorTupleStream
        implements TupleStream
{
    private final InlinedOperatorReader inlinedOperatorReader;

    public PartialInlinedOperatorTupleStream(InlinedOperatorReader inlinedOperatorReader)
    {
        this.inlinedOperatorReader = checkNotNull(inlinedOperatorReader, "inlinedOperatorReader is null");
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return inlinedOperatorReader.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return inlinedOperatorReader.getRange();
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        return inlinedOperatorReader.getResult().cursor(session);
    }
}
