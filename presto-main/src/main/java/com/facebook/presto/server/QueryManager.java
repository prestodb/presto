/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;

public interface QueryManager
{
    List<QueryInfo> getAllQueryInfo();

    QueryInfo createQuery(String query);

    QueryInfo createQueryFragment(Map<String, List<Split>> sourceSplits, PlanFragment planFragment);

    QueryInfo getQueryInfo(String queryId);

    State getQueryStatus(String queryId);

    List<Page> getQueryResults(String queryId, int maxPageCount, Duration maxWait)
            throws InterruptedException;

    void destroyQuery(String queryId);
}
