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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSessionManager;
import com.facebook.presto.sql.tree.SetVariable;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;

public class SetVariableExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SetVariableExecution.class);
    private final ConnectorSession session;
    private final SetVariable statement;
    private final ConnectorSessionManager connectorSessionManager;
    private final QueryStateMachine stateMachine;
    private Object data;

    SetVariableExecution(QueryId queryId,
            String query,
            ConnectorSession session,
            URI self,
            SetVariable statement,
            ConnectorSessionManager connectorSessionManager,
            Executor executor)
    {
        this.session = checkNotNull(session, "session is null");
        this.statement = statement;
        this.connectorSessionManager = connectorSessionManager;
        this.stateMachine = new QueryStateMachine(queryId, query, session, self, executor);
    }

    @Override
    public void start()
    {
        try {
            // transition to starting
            if (!stateMachine.starting()) {
                // query already started or finished
                return;
            }

            stateMachine.recordExecutionStart();

            SetVariable();

            stateMachine.finished();
        }
        catch (RuntimeException e) {
            fail(e);
        }
    }

    @Override
    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return stateMachine.waitForStateChange(currentState, maxWait);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void cancel()
    {
        stateMachine.cancel();
    }

    @Override
    public void fail(Throwable cause)
    {
        stateMachine.fail(cause);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public Iterable<List<Object>> getResultsForNonQueryStatement()
    {
        return ImmutableSet.<List<Object>>of(ImmutableList.<Object>of(data));
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return stateMachine.getQueryInfoWithoutDetails();
    }

    private void SetVariable()
    {
        String sessionId = checkNotNull(session.getSessionId(), "Cannot execute SET command without a session");
        ConnectorSession session = checkNotNull(connectorSessionManager.getSession(sessionId), "Session has expired");
        switch (statement.getType()) {
        case SET_VARIABLE:
            data = session.setConfig(statement.getVar().toString(), statement.getVal().toString());
            break;
        case UNSET_VARIABLE:
            data = session.unsetConfig(statement.getVar().toString());
            break;
        case SHOW_VARIABLE:
            data = checkNotNull(session.getConfig(statement.getVar().toString()), "No such variable in this session");
            break;
        case SHOW_ALL_VARIABLES:
            data = session.getConfigs().toString();
            break;
        default:
            break;
        }
    }

    public static class SetVariableExecutionFactory
            implements QueryExecutionFactory<SetVariableExecution>
    {
        private final LocationFactory locationFactory;
        private final ConnectorSessionManager connectorSessionManager;
        private final ExecutorService executor;

        @Inject
        SetVariableExecutionFactory(LocationFactory locationFactory,
                ConnectorSessionManager connectorSessionManager,
                @ForQueryExecution ExecutorService executor)
        {
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.connectorSessionManager = checkNotNull(connectorSessionManager, "connectorSessionManager is null");
            this.executor = checkNotNull(executor, "executor is null");
        }

        @Override
        public SetVariableExecution createQueryExecution(QueryId queryId, String query, ConnectorSession session, Statement statement)
        {
            return new SetVariableExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    (SetVariable) statement,
                    connectorSessionManager,
                    executor);
        }
    }
}
