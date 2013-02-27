/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.Session;

import java.util.List;

public interface QueryManager
{
    List<QueryInfo> getAllQueryInfo();

    QueryInfo getQueryInfo(String queryId, boolean forceRefresh);

    QueryInfo createQuery(Session session, String query);

    void cancelQuery(String queryId);

    void cancelStage(String queryId, String stageId);
}
