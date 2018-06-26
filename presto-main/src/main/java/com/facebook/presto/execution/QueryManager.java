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
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public interface QueryManager
{
    List<QueryInfo> getAllQueryInfo();

    void addOutputInfoListener(QueryId queryId, Consumer<QueryOutputInfo> listener);

    void addStateChangeListener(QueryId queryId, StateChangeListener<QueryState> listener);

    ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState);

    QueryInfo getQueryInfo(QueryId queryId);

    Optional<ResourceGroupId> getQueryResourceGroup(QueryId queryId);

    Plan getQueryPlan(QueryId queryId);

    Optional<QueryState> getQueryState(QueryId queryId);

    void recordHeartbeat(QueryId queryId);

    QueryId createQueryId();

    ListenableFuture<?> createQuery(QueryId queryId, SessionContext sessionContext, String query);

    void failQuery(QueryId queryId, Throwable cause);

    void cancelQuery(QueryId queryId);

    void cancelStage(StageId stageId);

    SqlQueryManagerStats getStats();
}
