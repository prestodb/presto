/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.PageBuffer;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public interface QueryDriverProvider
{
    int getChannelCount();

    List<TupleInfo> getTupleInfos();

    QueryDriver create(PageBuffer outputBuffer);
}
