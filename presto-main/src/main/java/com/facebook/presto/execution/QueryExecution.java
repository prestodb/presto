/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.Statement;
import io.airlift.units.Duration;

public interface QueryExecution
{
    QueryInfo getQueryInfo();

    Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException;

    void start();

    void cancel();

    void fail(Throwable cause);

    void cancelStage(StageId stageId);

    void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener);

    interface QueryExecutionFactory<T extends QueryExecution>
    {
        T createQueryExecution(QueryId queryId, String query, Session session, Statement statement);
    }
}
