/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.Statement;

public interface QueryExecution
{
    QueryInfo getQueryInfo();

    void start();

    void updateState(boolean forceRefresh);

    void cancel();

    void fail(Throwable cause);

    void cancelStage(StageId stageId);

    interface QueryExecutionFactory<T extends QueryExecution>
    {
        T createQueryExecution(QueryId queryId, String query, Session session, Statement statement);
    }
}
