/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

public interface RemoteTask
{
    String getTaskId();

    TaskInfo getTaskInfo();

    void start();

    void cancel();

    void updateState();
}
