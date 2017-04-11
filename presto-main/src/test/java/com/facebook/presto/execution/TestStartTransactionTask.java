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
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Isolation;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.TransactionAccessMode;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.INCOMPATIBLE_CLIENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_TRANSACTION_MODE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
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
            throws Exception
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testNonTransactionalClient()
            throws Exception
    {
        Session session = sessionBuilder().build();
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "START TRANSACTION", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor, metadata);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        try {
            try {
                getFutureValue(new StartTransactionTask().execute(new StartTransaction(ImmutableList.of()), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()));
                fail();
            }
            catch (CompletionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INCOMPATIBLE_CLIENT.toErrorCode());
        }
        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertFalse(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());
    }

    @Test
    public void testNestedTransaction()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);

        Session session = sessionBuilder()
                .setTransactionId(TransactionId.create())
                .setClientTransactionSupport()
                .build();
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "START TRANSACTION", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor, metadata);

        try {
            try {
                getFutureValue(new StartTransactionTask().execute(new StartTransaction(ImmutableList.of()), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()));
                fail();
            }
            catch (CompletionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        }
        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertFalse(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransaction()
            throws Exception
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "START TRANSACTION", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor, metadata);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask().execute(new StartTransaction(ImmutableList.of()), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()));
        assertFalse(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertTrue(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        TransactionInfo transactionInfo = transactionManager.getTransactionInfo(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().get());
        assertFalse(transactionInfo.isAutoCommitContext());
    }

    @Test
    public void testStartTransactionExplicitModes()
            throws Exception
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "START TRANSACTION", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor, metadata);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask().execute(
                new StartTransaction(ImmutableList.of(new Isolation(Isolation.Level.SERIALIZABLE), new TransactionAccessMode(true))),
                transactionManager,
                metadata,
                new AllowAllAccessControl(),
                stateMachine,
                emptyList()));
        assertFalse(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertTrue(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        TransactionInfo transactionInfo = transactionManager.getTransactionInfo(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().get());
        assertEquals(transactionInfo.getIsolationLevel(), IsolationLevel.SERIALIZABLE);
        assertTrue(transactionInfo.isReadOnly());
        assertFalse(transactionInfo.isAutoCommitContext());
    }

    @Test
    public void testStartTransactionTooManyIsolationLevels()
            throws Exception
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "START TRANSACTION", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor, metadata);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        try {
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
            catch (CompletionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_TRANSACTION_MODE);
        }
        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertFalse(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransactionTooManyAccessModes()
            throws Exception
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "START TRANSACTION", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor, metadata);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        try {
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
            catch (CompletionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_TRANSACTION_MODE);
        }
        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertFalse(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransactionIdleExpiration()
            throws Exception
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = TransactionManager.create(
                new TransactionManagerConfig()
                        .setIdleTimeout(new Duration(1, TimeUnit.MICROSECONDS)) // Fast idle timeout
                        .setIdleCheckInterval(new Duration(10, TimeUnit.MILLISECONDS)),
                scheduledExecutor,
                new CatalogManager(),
                executor);
        AccessControl accessControl = new AccessControlManager(transactionManager);
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "START TRANSACTION", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor, metadata);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask().execute(
                new StartTransaction(ImmutableList.of()),
                transactionManager,
                metadata,
                new AllowAllAccessControl(),
                stateMachine,
                emptyList()));
        assertFalse(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertTrue(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());

        long start = System.nanoTime();
        while (!transactionManager.getAllTransactionInfos().isEmpty()) {
            if (Duration.nanosSince(start).toMillis() > 10_000) {
                fail("Transaction did not expire in the allotted time");
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private static SessionBuilder sessionBuilder()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME);
    }
}
