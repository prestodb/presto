package com.facebook.presto.operator.inlined;

import com.facebook.presto.block.TupleStreamPosition;

public interface InlinedOperatorWriter
{
    void process(TupleStreamPosition tupleStreamPosition);
    void finished();
}
