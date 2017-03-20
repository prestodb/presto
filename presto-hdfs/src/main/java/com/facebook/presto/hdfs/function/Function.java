package com.facebook.presto.hdfs.function;

/**
 * presto-root
 *
 * @author Jelly
 */
public interface Function
{
    long apply(String v);
    long apply(int v);
    long apply(long v);
}
