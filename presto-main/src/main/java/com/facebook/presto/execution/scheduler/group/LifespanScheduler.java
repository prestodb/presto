package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.scheduler.SourceScheduler;
import com.google.common.util.concurrent.SettableFuture;

public interface LifespanScheduler
{
    void scheduleInitial(SourceScheduler scheduler);

    void onLifespanFinished(Iterable<Lifespan> newlyCompletedDriverGroups);

    SettableFuture schedule(SourceScheduler scheduler);
}
