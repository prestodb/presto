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

import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface QueryExecution
{
    QueryId getQueryId();

    QueryInfo getQueryInfo();

    QueryState getState();

    ListenableFuture<QueryState> getStateChange(QueryState currentState);

    ListenableFuture<QueryOutputInfo> getOutputInfo();

    Optional<ResourceGroupId> getResourceGroup();

    void setResourceGroup(ResourceGroupId resourceGroupId);

    Plan getQueryPlan();

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

    Optional<QueryType> getQueryType();

    class QueryOutputInfo
    {
        private final List<String> columnNames;
        private final List<Type> columnTypes;
        private final SetMultimap<OutputBufferId, URI> bufferLocations;

        public QueryOutputInfo(List<String> columnNames, List<Type> columnTypes, SetMultimap<OutputBufferId, URI> bufferLocations)
        {
            this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.bufferLocations = ImmutableSetMultimap.copyOf(requireNonNull(bufferLocations, "bufferLocations is null"));
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }

        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }

        public SetMultimap<OutputBufferId, URI> getBufferLocations()
        {
            return bufferLocations;
        }
    }
}
