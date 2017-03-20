package com.facebook.presto.hdfs.function;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.charset.Charset;

/**
 * presto-root
 *
 * Using MurmurHash function
 * @author Jelly
 */
@ThreadSafe
public final class Function0 extends com.facebook.presto.hdfs.function.HashFunction
{
    private final int seed;
    private final HashFunction hasher;

    public Function0(int seed)
    {
        this.seed = seed;
        hasher = Hashing.murmur3_128(seed);
    }

    public Function0()
    {
        this.seed = 1318007700;
        hasher = Hashing.murmur3_128(seed);
    }

    @Override
    public long apply(String v)
    {
        return hasher.hashString(v.subSequence(0, v.length()), Charset.defaultCharset()).asLong();
    }

    @Override
    public long apply(int v)
    {
        return hasher.hashInt(v).asLong();
    }

    @Override
    public long apply(long v)
    {
        return hasher.hashLong(v).asLong();
    }
}
