package com.facebook.presto.execution;

import com.google.common.util.concurrent.ListenableFuture;

public interface SplitRunner
{
    boolean isFinished();

    ListenableFuture<?> process()
            throws Exception;
}
