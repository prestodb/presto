/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import java.util.List;

public interface StageExecution
{
    String getStageId();

    StageInfo getStageInfo();

    List<StageExecution> getSubStages();

    ExchangePlanFragmentSource getExchangeSourceFor(String outputId);

    void startTasks(List<String> outputIds);

    void updateState();

    void cancel();
}
