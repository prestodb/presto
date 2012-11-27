/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.split.PlanFragment;
import com.facebook.presto.split.Split;

import java.util.List;
import java.util.Map;

public interface QueryManager {
    QueryInfo createQuery(String query);

    QueryInfo createQueryFragment(Map<String, List<Split>> sourceSplits, PlanFragment planFragment);

    List<Page> getQueryResults(String queryId, int maxPageCount)
            throws InterruptedException;

    void destroyQuery(String queryId);
}
