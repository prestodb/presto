/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

interface QueryTask
{
    String getTaskId();

    QueryTaskInfo getQueryTaskInfo();

    void cancel();
}
