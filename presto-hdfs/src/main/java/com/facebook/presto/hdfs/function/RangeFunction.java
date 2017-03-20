package com.facebook.presto.hdfs.function;

import com.facebook.presto.hdfs.exception.UnSupportedFunctionException;

/**
 * presto-root
 *
 * @author Jelly
 */
public abstract class RangeFunction implements Function
{
    private final long start;
    private final long stride;
    private final long end;

    public RangeFunction(long start, long stride, long end)
    {
        this.start = start;
        this.stride = stride;
        this.end = end;
    }

    @Override
    public long apply(String v)
    {
        throw new UnSupportedFunctionException("Unsupported range function on string value");
    }

    @Override
    public abstract long apply(int v);

    @Override
    public abstract long apply(long v);
}
