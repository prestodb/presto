/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

public interface QueryDriver
{
    void start();

    boolean isDone();

    void cancel();
}
