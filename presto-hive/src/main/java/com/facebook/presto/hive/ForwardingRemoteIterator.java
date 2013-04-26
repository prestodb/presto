package com.facebook.presto.hive;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public abstract class ForwardingRemoteIterator<T>
        implements RemoteIterator<T>
{
    private final RemoteIterator<T> remoteIterator;

    public ForwardingRemoteIterator(RemoteIterator<T> remoteIterator)
    {
        this.remoteIterator = Preconditions.checkNotNull(remoteIterator, "remoteIterator is null");
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        return remoteIterator.hasNext();
    }

    @Override
    public T next()
            throws IOException
    {
        return remoteIterator.next();
    }
}
