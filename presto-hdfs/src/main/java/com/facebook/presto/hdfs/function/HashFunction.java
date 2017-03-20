package com.facebook.presto.hdfs.function;

/**
 * presto-root
 *
 * @author Jelly
 */
public abstract class HashFunction implements Function
{
    @Override
    public abstract long apply(String v);

    @Override
    public abstract long apply(int v);

    @Override
    public abstract long apply(long v);
}
