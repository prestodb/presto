/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.TupleInfo;

public interface QueryDriverProvider {
    QueryDriver create(QueryState queryState, TupleInfo info);
}
