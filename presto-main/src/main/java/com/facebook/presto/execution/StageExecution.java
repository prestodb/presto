/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.split.Split;

import java.util.List;
import java.util.Set;

public interface StageExecution
{
    String getStageId();

    StageInfo getStageInfo();

    List<StageExecution> getSubStages();

    Set<Split> getSplitsForExchange(String outputId);

    void startTasks(List<String> outputIds);

    void updateState();

    void cancel();
}
