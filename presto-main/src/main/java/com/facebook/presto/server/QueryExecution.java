/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

public interface QueryExecution
{
    String getQueryId();

    QueryInfo getQueryInfo();

    void start();

    void cancel();
}
