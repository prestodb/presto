/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.Set;

public interface RemoteTask
{

    TaskInfo getTaskInfo();

    void start();

    void addSplit(Split split);

    void noMoreSplits();

    void addExchangeLocations(Multimap<PlanNodeId, URI> exchangeLocations, boolean noMore);

    void addOutputBuffers(Set<String> outputBuffers, boolean noMore);

    void addStateChangeListener(StateChangeListener<TaskInfo> stateChangeListener);

    void cancel();

    int getQueuedSplits();

    Duration waitForTaskToFinish(Duration maxWait)
            throws InterruptedException;
}
