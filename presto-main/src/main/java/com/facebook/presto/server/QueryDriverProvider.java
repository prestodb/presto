/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

public interface QueryDriverProvider
{
    int getChannelCount();

    QueryDriver create(QueryState queryState);
}
