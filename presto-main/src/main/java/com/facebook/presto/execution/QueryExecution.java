/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

public interface QueryExecution
{
    String getQueryId();

    QueryInfo getQueryInfo();

    void start();

    void updateState(boolean forceRefresh);

    void cancel();

    void fail(Throwable cause);

    void cancelStage(String stageId);
}
