/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;

import java.util.List;

public interface QueryManager {
    QueryInfo createQuery(String query);

    List<Page> getQueryResults(String queryId, int maxPageCount)
            throws InterruptedException;

    void destroyQuery(String queryId);
}
