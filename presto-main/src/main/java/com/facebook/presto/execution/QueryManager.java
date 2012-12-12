/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import java.util.List;

public interface QueryManager
{
    List<QueryInfo> getAllQueryInfo();

    QueryInfo getQueryInfo(String queryId, boolean forceRefresh);

    QueryInfo createQuery(String query);

    void cancelQuery(String queryId);
}
