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

import com.facebook.presto.execution.QueryExecution.QueryOutputInfo;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;

public interface QueryManager
{
    List<BasicQueryInfo> getQueries();

    /**
     * Add a listener that fires once the query output locations are known.
     *
     * @throws NoSuchElementException if query does not exist
     */
    void addOutputInfoListener(QueryId queryId, Consumer<QueryOutputInfo> listener)
            throws NoSuchElementException;

    /**
     * Add a listener that fires each time the query state changes.
     *
     * @throws NoSuchElementException if query does not exist
     */
    void addStateChangeListener(QueryId queryId, StateChangeListener<QueryState> listener)
            throws NoSuchElementException;

    /**
     * Gets a future that completes when the query changes from the specified current state
     * or immediately if the query is already in a final state.  If the query does not exist,
     * the future will contain a {@link NoSuchElementException}
     */
    ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState);

    /**
     * @throws NoSuchElementException if query does not exist
     */
    BasicQueryInfo getQueryInfo(QueryId queryId)
            throws NoSuchElementException;

    /**
     * @throws NoSuchElementException if query does not exist
     */
    QueryInfo getFullQueryInfo(QueryId queryId)
            throws NoSuchElementException;

    /**
     * Gets to the resource group assigned to the query.  If the query does not exist or
     * a resource group has not been assigned, the result will be empty.
     */
    Optional<ResourceGroupId> getQueryResourceGroup(QueryId queryId);

    /**
     * @throws NoSuchElementException if query does not exist
     */
    QueryState getQueryState(QueryId queryId)
            throws NoSuchElementException;

    /**
     * Updates the client heartbeat time, to prevent the query from be automatically purged.
     * If the query does not exist, the call is ignored.
     */
    void recordHeartbeat(QueryId queryId);

    QueryId createQueryId();

    /**
     * Creates a new query.  This method may be called multiple times for the same query id.  The
     * the first call will be accepted, and the other calls will be ignored.
     */
    ListenableFuture<?> createQuery(QueryId queryId, SessionContext sessionContext, String query);

    /**
     * Attempts to fail the query for the specified reason.  If the query is already in a final
     * state, the call is ignored.  If the query does not exist, the call is ignored.
     */
    void failQuery(QueryId queryId, Throwable cause);

    /**
     * Attempts to fail the query due to a user cancellation.  If the query is already in a final
     * state, the call is ignored.  If the query does not exist, the call is ignored.
     */
    void cancelQuery(QueryId queryId);

    /**
     * Attempts to cancel the stage and continue the query.  If the stage is already in a final
     * state, the call is ignored.  If the query does not exist, the call is ignored.
     */
    void cancelStage(StageId stageId);

    SqlQueryManagerStats getStats();
}
