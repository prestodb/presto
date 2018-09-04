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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public interface QueryExecution
        extends ManagedQueryExecution
{
    QueryId getQueryId();

    QueryState getState();

    ListenableFuture<QueryState> getStateChange(QueryState currentState);

    void addOutputInfoListener(Consumer<QueryOutputInfo> listener);

    Optional<ResourceGroupId> getResourceGroup();

    Plan getQueryPlan();

    QueryInfo getQueryInfo();

    VersionedMemoryPoolId getMemoryPool();

    void setMemoryPool(VersionedMemoryPoolId poolId);

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

    /**
     * Output schema and buffer URIs for query.  The info will always contain column names and types.  Buffer locations will always
     * contain the full location set, but may be empty.  Users of this data should keep a private copy of the seen buffers to
     * handle out of order events from the listener.  Once noMoreBufferLocations is set the locations will never change, and
     * it is guaranteed that all previously sent locations are contained in the buffer locations.
     */
    class QueryOutputInfo
    {
        private final List<String> columnNames;
        private final List<Type> columnTypes;
        private final Set<URI> bufferLocations;
        private final boolean noMoreBufferLocations;

        public QueryOutputInfo(List<String> columnNames, List<Type> columnTypes, Set<URI> bufferLocations, boolean noMoreBufferLocations)
        {
            this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.bufferLocations = ImmutableSet.copyOf(requireNonNull(bufferLocations, "bufferLocations is null"));
            this.noMoreBufferLocations = noMoreBufferLocations;
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }

        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }

        public Set<URI> getBufferLocations()
        {
            return bufferLocations;
        }

        public boolean isNoMoreBufferLocations()
        {
            return noMoreBufferLocations;
        }
    }
}
