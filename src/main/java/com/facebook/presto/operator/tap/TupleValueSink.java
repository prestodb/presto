package com.facebook.presto.operator.tap;

import com.facebook.presto.block.TupleStreamPosition;

public interface TupleValueSink
{
    /**
     * Implementations are responsible for processing the entire Tuple value sequence associated with the TupleStreamPosition
     * [current position until current value position end]
     */
    void process(TupleStreamPosition tupleStreamPosition);
    void finished();
}
