package com.facebook.presto.operator.inlined;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;

public interface InlinedOperatorReader
{
    boolean isFinished();
    TupleInfo getTupleInfo();
    Range getRange();

    /**
     * Returns the current result as a TupleStream (can be partial result if not yet finished)
     */
    TupleStream getResult();
}
