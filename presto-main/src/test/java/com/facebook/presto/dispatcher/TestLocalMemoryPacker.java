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
package com.facebook.presto.dispatcher;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestLocalMemoryPacker
{
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));
    private final TransactionManager transactionManager = createTestTransactionManager();
    private final Session session = createSession();

    @Test
    public void testSingleQuery()
            throws ExecutionException, InterruptedException
    {
        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                () -> 1024L,
                () -> 512L,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(new DataSize(0, BYTE))
                        .setAverageQueryPeakMemory(new DataSize(0, BYTE)));

        packer.initialize();

        LocalDispatchQuery query = new LocalDispatchQuery(
                createQueryStateMachine(session, transactionManager),
                immediateFuture(new TestQueryExecution()),
                new CoordinatorLocation(Optional.of(URI.create("http://localhost")), Optional.empty()),
                () -> null,
                packer,
                (queryExecution -> null));

        Future<?> future = SettableFuture.create();
        packer.addToQueue(query, () -> ((SettableFuture<?>) future).set(null));
        future.get();
    }

    private QueryStateMachine createQueryStateMachine(Session session, TransactionManager transactionManager)
    {
        return QueryStateMachine.begin(
                "SELECT 1",
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                true,
                transactionManager,
                new AccessControlManager(transactionManager),
                executor,
                metadata,
                WarningCollector.NOOP);
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();
    }

    private static class TestQueryExecution
            implements QueryExecution
    {
        @Override
        public QueryState getState()
        {
            return null;
        }

        @Override
        public ListenableFuture<QueryState> getStateChange(QueryState currentState)
        {
            return null;
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
        {

        }

        @Override
        public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
        {

        }

        @Override
        public Plan getQueryPlan()
        {
            return null;
        }

        @Override
        public BasicQueryInfo getBasicQueryInfo()
        {
            return null;
        }

        @Override
        public QueryInfo getQueryInfo()
        {
            return null;
        }

        @Override
        public String getSlug()
        {
            return null;
        }

        @Override
        public Duration getTotalCpuTime()
        {
            return null;
        }

        @Override
        public DataSize getUserMemoryReservation()
        {
            return null;
        }

        @Override
        public DataSize getTotalMemoryReservation()
        {
            return null;
        }

        @Override
        public VersionedMemoryPoolId getMemoryPool()
        {
            return null;
        }

        @Override
        public void setMemoryPool(VersionedMemoryPoolId poolId)
        {

        }

        @Override
        public void start()
        {

        }

        @Override
        public void cancelQuery()
        {

        }

        @Override
        public void cancelStage(StageId stageId)
        {

        }

        @Override
        public void recordHeartbeat()
        {

        }

        @Override
        public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
        {

        }

        @Override
        public QueryId getQueryId()
        {
            return null;
        }

        @Override
        public boolean isDone()
        {
            return false;
        }

        @Override
        public Session getSession()
        {
            return null;
        }

        @Override
        public DateTime getCreateTime()
        {
            return null;
        }

        @Override
        public Optional<DateTime> getExecutionStartTime()
        {
            return Optional.empty();
        }

        @Override
        public DateTime getLastHeartbeat()
        {
            return null;
        }

        @Override
        public Optional<DateTime> getEndTime()
        {
            return Optional.empty();
        }

        @Override
        public void fail(Throwable cause)
        {

        }

        @Override
        public void pruneInfo()
        {

        }
    }
}
