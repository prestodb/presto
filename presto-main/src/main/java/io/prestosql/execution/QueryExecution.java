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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.QueryTracker.TrackedQuery;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Plan;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public interface QueryExecution
        extends ManagedQueryExecution, TrackedQuery
{
    QueryState getState();

    ListenableFuture<QueryState> getStateChange(QueryState currentState);

    void addOutputInfoListener(Consumer<QueryOutputInfo> listener);

    Plan getQueryPlan();

    QueryInfo getQueryInfo();

    VersionedMemoryPoolId getMemoryPool();

    void setMemoryPool(VersionedMemoryPoolId poolId);

    void cancelQuery();

    void cancelStage(StageId stageId);

    void recordHeartbeat();

    /**
     * Add a listener for the final query info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener);

    interface QueryExecutionFactory<T extends QueryExecution>
    {
        T createQueryExecution(String query, Session session, PreparedQuery preparedQuery, ResourceGroupId resourceGroup, WarningCollector warningCollector);
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
