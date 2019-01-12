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
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.Session.SessionBuilder;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.Isolation;
import io.prestosql.sql.tree.StartTransaction;
import io.prestosql.sql.tree.TransactionAccessMode;
import io.prestosql.transaction.InMemoryTransactionManager;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionInfo;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.transaction.TransactionManagerConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.spi.StandardErrorCode.INCOMPATIBLE_CLIENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_TRANSACTION_MODE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestStartTransactionTask
{
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("scheduled-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testNonTransactionalClient()
    {
        Session session = sessionBuilder().build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        try {
            getFutureValue(new StartTransactionTask().execute(new StartTransaction(ImmutableList.of()), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()));
            fail();
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INCOMPATIBLE_CLIENT.toErrorCode());
        }
        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
    }

    @Test
    public void testNestedTransaction()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Session session = sessionBuilder()
                .setTransactionId(TransactionId.create())
                .setClientTransactionSupport()
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);

        try {
            getFutureValue(new StartTransactionTask().execute(new StartTransaction(ImmutableList.of()), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()));
            fail();
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        }
        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransaction()
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask().execute(new StartTransaction(ImmutableList.of()), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()));
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertTrue(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        TransactionInfo transactionInfo = transactionManager.getTransactionInfo(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().get());
        assertFalse(transactionInfo.isAutoCommitContext());
    }

    @Test
    public void testStartTransactionExplicitModes()
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask().execute(
                new StartTransaction(ImmutableList.of(new Isolation(Isolation.Level.SERIALIZABLE), new TransactionAccessMode(true))),
                transactionManager,
                metadata,
                new AllowAllAccessControl(),
                stateMachine,
                emptyList()));
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertTrue(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        TransactionInfo transactionInfo = transactionManager.getTransactionInfo(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().get());
        assertEquals(transactionInfo.getIsolationLevel(), IsolationLevel.SERIALIZABLE);
        assertTrue(transactionInfo.isReadOnly());
        assertFalse(transactionInfo.isAutoCommitContext());
    }

    @Test
    public void testStartTransactionTooManyIsolationLevels()
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        try {
            getFutureValue(new StartTransactionTask().execute(
                    new StartTransaction(ImmutableList.of(new Isolation(Isolation.Level.READ_COMMITTED), new Isolation(Isolation.Level.READ_COMMITTED))),
                    transactionManager,
                    metadata,
                    new AllowAllAccessControl(),
                    stateMachine,
                    emptyList()));
            fail();
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_TRANSACTION_MODE);
        }
        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransactionTooManyAccessModes()
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        try {
            getFutureValue(new StartTransactionTask().execute(
                    new StartTransaction(ImmutableList.of(new TransactionAccessMode(true), new TransactionAccessMode(true))),
                    transactionManager,
                    metadata,
                    new AllowAllAccessControl(),
                    stateMachine,
                    emptyList()));
            fail();
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_TRANSACTION_MODE);
        }
        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransactionIdleExpiration()
            throws Exception
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig()
                        .setIdleTimeout(new Duration(1, TimeUnit.MICROSECONDS)) // Fast idle timeout
                        .setIdleCheckInterval(new Duration(10, TimeUnit.MILLISECONDS)),
                scheduledExecutor,
                new CatalogManager(),
                executor);
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask().execute(
                new StartTransaction(ImmutableList.of()),
                transactionManager,
                metadata,
                new AllowAllAccessControl(),
                stateMachine,
                emptyList()));
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertTrue(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());

        long start = System.nanoTime();
        while (!transactionManager.getAllTransactionInfos().isEmpty()) {
            if (Duration.nanosSince(start).toMillis() > 10_000) {
                fail("Transaction did not expire in the allotted time");
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private QueryStateMachine createQueryStateMachine(String query, Session session, TransactionManager transactionManager)
    {
        return QueryStateMachine.begin(
                query,
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

    private static SessionBuilder sessionBuilder()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME);
    }
}
