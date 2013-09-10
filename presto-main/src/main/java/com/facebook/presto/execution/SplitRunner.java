package com.facebook.presto.execution;

import com.google.common.util.concurrent.ListenableFuture;

public interface SplitRunner
{
    void initialize();

    boolean isFinished();

    ListenableFuture<?> process()
            throws Exception;
}
