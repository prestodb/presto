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

import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Optional;

public interface QueryExecution
{
    QueryId getQueryId();

    QueryInfo getQueryInfo();

    QueryState getState();

    Optional<ResourceGroupId> getResourceGroup();

    void setResourceGroup(ResourceGroupId resourceGroupId);

    Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException;

    VersionedMemoryPoolId getMemoryPool();

    void setMemoryPool(VersionedMemoryPoolId poolId);

    long getTotalMemoryReservation();

    Duration getTotalCpuTime();

    Session getSession();

    void start();

    void fail(Throwable cause);

    void cancelQuery();

    void cancelStage(StageId stageId);

    void recordHeartbeat();

    // XXX: This should be removed when the client protocol is improved, so that we don't need to hold onto so much query history
    void pruneInfo();

    void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener);

    void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener);

    interface QueryExecutionFactory<T extends QueryExecution>
    {
        T createQueryExecution(QueryId queryId, String query, Session session, Statement statement, List<Expression> parameters);
    }
}
