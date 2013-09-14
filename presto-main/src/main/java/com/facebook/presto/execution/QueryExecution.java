/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    void recordHeartbeat();

    void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener);

    interface QueryExecutionFactory<T extends QueryExecution>
    {
        T createQueryExecution(QueryId queryId, String query, Session session, Statement statement);
    }
}
