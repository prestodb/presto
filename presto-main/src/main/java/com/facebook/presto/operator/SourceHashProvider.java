/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Provider;
import java.util.List;

@ThreadSafe
public class SourceHashProvider
        implements Provider<SourceHash>
{
    private final Operator source;
    private final int hashChannel;
    private final int expectedPositions;
    private final OperatorStats operatorStats;

    @GuardedBy("this")
    private SourceHash sourceHash;

    public SourceHashProvider(Operator source, int hashChannel, int expectedPositions, OperatorStats operatorStats)
    {
        this.source = source;
        this.hashChannel = hashChannel;
        this.expectedPositions = expectedPositions;
        this.operatorStats = operatorStats;
    }

    public int getChannelCount()
    {
        return source.getChannelCount();
    }

    public int getHashChannel()
    {
        return hashChannel;
    }

    public List<TupleInfo> getTupleInfos()
    {
        return source.getTupleInfos();
    }

    @Override
    public synchronized SourceHash get()
    {
        if (sourceHash == null) {
            sourceHash = new SourceHash(source, hashChannel, expectedPositions, operatorStats);
        }
        return new SourceHash(sourceHash);
    }
}
