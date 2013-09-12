package com.facebook.presto.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

public interface SplitRunner
{
    void initialize();

    boolean isFinished();

    ListenableFuture<?> processFor(Duration duration)
            throws Exception;
}
