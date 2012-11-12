/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public interface Operator
        extends Iterable<Page>
{
    int getChannelCount();

    List<TupleInfo> getTupleInfos();
}
