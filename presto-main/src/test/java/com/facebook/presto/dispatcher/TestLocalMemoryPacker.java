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
import com.facebook.presto.Session.ResourceEstimateBuilder;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestLocalMemoryPacker
{
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private final ExecutorService queryExecutor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));
    private final TransactionManager transactionManager = createTestTransactionManager();

    @Test(timeOut = 10000)
    public void testZeroMemoryQuery()
            throws ExecutionException, InterruptedException
    {
        TestingClusterMemoryPoolManager memoryManager = new TestingClusterMemoryPoolManager(1024);

        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                memoryManager,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(succinctBytes(0))
                        .setAverageQueryPeakMemory(succinctBytes(0)));

        packer.initialize();

        Future<?> future = runQuery(succinctMillis(1000), succinctBytes(0), new ResourceEstimateBuilder().build(), packer, memoryManager);
        future.get();
    }

    @Test(timeOut = 10000)
    public void testZeroRuntimeQueries()
            throws ExecutionException, InterruptedException
    {
        TestingClusterMemoryPoolManager memoryManager = new TestingClusterMemoryPoolManager(1024);

        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                memoryManager,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(succinctBytes(0))
                        .setAverageQueryPeakMemory(succinctBytes(0))
                        .setTotalToUserMemoryRatio(1)
                        .setQueryWarmupTime(succinctNanos(0)));

        packer.initialize();

        Future<?> query1 = runQuery(succinctNanos(0), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);
        query1.get();

        Future<?> query2 = runQuery(succinctNanos(0), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);
        query2.get();
    }

    @Test(timeOut = 1000)
    public void testSerial()
            throws Exception
    {
        TestingClusterMemoryPoolManager memoryManager = new TestingClusterMemoryPoolManager(512);

        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                memoryManager,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(succinctBytes(0))
                        .setAverageQueryPeakMemory(succinctBytes(0))
                        .setTotalToUserMemoryRatio(1)
                        .setQueryWarmupTime(succinctMillis(0)));

        packer.initialize();

        Future<?> query1 = runQuery(succinctMillis(100), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);
        query1.get();

        Future<?> query2 = runQuery(succinctMillis(100), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);
        query2.get();
    }

    @Test(timeOut = 10000)
    public void testParallel()
            throws Exception
    {
        TestingClusterMemoryPoolManager memoryManager = new TestingClusterMemoryPoolManager(1024);

        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                memoryManager,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(succinctBytes(0))
                        .setAverageQueryPeakMemory(succinctBytes(0))
                        .setTotalToUserMemoryRatio(1)
                        .setQueryWarmupTime(succinctMillis(0)));

        packer.initialize();

        Future<?> query1 = runQuery(succinctMillis(1000), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);
        Future<?> query2 = runQuery(succinctMillis(1000), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);
        query1.get();
        query2.get(100, MILLISECONDS);
    }

    @Test(timeOut = 10000)
    public void testWarmup()
            throws Exception
    {
        TestingClusterMemoryPoolManager memoryManager = new TestingClusterMemoryPoolManager(1024);
        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                memoryManager,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(new DataSize(0, BYTE))
                        .setAverageQueryPeakMemory(new DataSize(0, BYTE))
                        .setTotalToUserMemoryRatio(1)
                        .setQueryWarmupTime(succinctMillis(1000)));

        packer.initialize();

        // simulate the query memory not showing up for a while
        Future<?> query1 = runQuery(succinctMillis(1000), succinctBytes(512), memoryEstimateBytes(1024), packer, memoryManager);
        Future<?> query2 = runQuery(succinctMillis(1), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);

        try {
            query1.get(10, MILLISECONDS);
            throw new Exception("Query should not begin until warmup period expires");
        }
        catch (TimeoutException ignored) {
        }

        query2.get();
    }

    @Test(timeOut = 10000)
    public void testQueryCompletionBeforeWarmup()
            throws Exception
    {
        TestingClusterMemoryPoolManager memoryManager = new TestingClusterMemoryPoolManager(1024);
        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                memoryManager,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(new DataSize(0, BYTE))
                        .setAverageQueryPeakMemory(new DataSize(0, BYTE))
                        .setTotalToUserMemoryRatio(1)
                        .setQueryWarmupTime(succinctMillis(1000)));

        packer.initialize();

        runQuery(succinctMillis(1), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);
        Future<?> query2 = runQuery(succinctMillis(1), succinctBytes(512), memoryEstimateBytes(512), packer, memoryManager);

        query2.get(10, MILLISECONDS);
    }

    @Test(timeOut = 10000)
    public void testSimple()
            throws Exception
    {
        TestingClusterMemoryPoolManager memoryManager = new TestingClusterMemoryPoolManager(1024);

        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                memoryManager,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(succinctBytes(0))
                        .setAverageQueryPeakMemory(succinctBytes(0))
                        .setTotalToUserMemoryRatio(1)
                        .setQueryWarmupTime(succinctMillis(100)));

        packer.initialize();

        ResourceEstimates estimates = new ResourceEstimateBuilder()
                .setPeakMemory(new DataSize(512, BYTE))
                .build();

        memoryManager.reserve(1024);
        Future<?> query1 = runQuery(succinctMillis(100), succinctBytes(512), estimates, packer, memoryManager);

        try {
            query1.get(10, MILLISECONDS);
            throw new Exception("Query should not begin until memory is available");
        }
        catch (TimeoutException ignored) {
        }

        memoryManager.free(768);

        try {
            query1.get(10, MILLISECONDS);
            throw new Exception("Query should not begin until enough memory is available");
        }
        catch (TimeoutException ignored) {
        }

        memoryManager.free(256);
        query1.get();
    }

    @Test
    public void testMultiple()
            throws Exception
    {
        TestingClusterMemoryPoolManager memoryManager = new TestingClusterMemoryPoolManager(1024);

        LocalDispatchMemoryPacker packer = new LocalDispatchMemoryPacker(
                memoryManager,
                new LocalDispatchMemoryPackerConfig()
                        .setMinimumSystemMemory(succinctBytes(0))
                        .setAverageQueryPeakMemory(succinctBytes(512))
                        .setTotalToUserMemoryRatio(1)
                        .setQueryWarmupTime(succinctMillis(0)));

        packer.initialize();

        ResourceEstimates estimate512 = memoryEstimateBytes(512);

        Future<?> query1 = runQuery(succinctMillis(100), succinctBytes(512), estimate512, packer, memoryManager);
        Future<?> query2 = runQuery(succinctMillis(1000), succinctBytes(512), estimate512, packer, memoryManager);

        query2.get();

        Future<?> query3 = runQuery(succinctMillis(100), succinctBytes(512), estimate512, packer, memoryManager);
        query3.get();

        Future<?> query4 = runQuery(succinctMillis(100), succinctBytes(1024), memoryEstimateBytes(1024), packer, memoryManager);
        query1.get();
        query4.get();
    }

    private static ResourceEstimates memoryEstimateBytes(long bytes)
    {
        return new ResourceEstimateBuilder()
                .setPeakMemory(succinctBytes(bytes))
                .build();
    }

    private static Duration succinctMillis(long millis)
    {
        return new Duration(millis, MILLISECONDS);
    }

    private Future<?> runQuery(Duration runtime, DataSize totalMemory, ResourceEstimates estimates, LocalDispatchMemoryPacker packer, TestingClusterMemoryPoolManager memoryManager)
    {
        LocalDispatchQuery query = createQuery(runtime, totalMemory, estimates, packer, memoryManager);
        SettableFuture<?> future = SettableFuture.create();

        query.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                future.set(null);
            }
        });

        query.startWaitingForResources();

        return future;
    }

    private LocalDispatchQuery createQuery(Duration queryRuntime, DataSize totalMemory, ResourceEstimates estimates, LocalDispatchMemoryPacker packer, TestingClusterMemoryPoolManager memoryManager)
    {
        QueryStateMachine stateMachine = createQueryStateMachine(sessionBuilder().setResourceEstimates(estimates).build(), transactionManager);
        return new LocalDispatchQuery(
                stateMachine,
                immediateFuture(new TestingQueryExecution(queryRuntime, totalMemory, stateMachine, memoryManager)),
                new CoordinatorLocation(Optional.of(URI.create("http://localhost")), Optional.empty()),
                () -> immediateFuture(null),
                packer,
                queryExecution -> {
                    queryExecution.start();
                    return immediateFuture(null);
                });
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
                queryExecutor,
                metadata,
                WarningCollector.NOOP);
    }

    private static Session.SessionBuilder sessionBuilder()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME);
    }
}
