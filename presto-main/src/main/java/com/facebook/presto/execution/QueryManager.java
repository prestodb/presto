/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.Session;

import java.util.List;

public interface QueryManager
{
    List<QueryInfo> getAllQueryInfo();

    QueryInfo getQueryInfo(QueryId queryId, boolean forceRefresh);

    QueryInfo createQuery(Session session, String query);

    void cancelQuery(QueryId queryId);

    void cancelStage(StageId stageId);
}
