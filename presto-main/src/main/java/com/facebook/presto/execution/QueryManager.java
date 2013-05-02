/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.Session;
import io.airlift.units.Duration;

import java.util.List;

public interface QueryManager
{
    List<QueryInfo> getAllQueryInfo();

    Duration waitForStateChange(QueryId queryId, QueryState currentState, Duration maxWait)
            throws InterruptedException;

    QueryInfo getQueryInfo(QueryId queryId);

    QueryInfo createQuery(Session session, String query);

    void cancelQuery(QueryId queryId);

    void cancelStage(StageId stageId);
}
