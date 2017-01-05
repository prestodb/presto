
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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Throwables;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.spi.StandardErrorCode.NOT_IN_TRANSACTION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestRollbackTask
{
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testRollback()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);

        Session session = sessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "ROLLBACK", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor);
        assertTrue(stateMachine.getSession().getTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        new RollbackTask().execute(new Rollback(), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()).join();
        assertTrue(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertFalse(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
    }

    @Test
    public void testNoTransactionRollback()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);

        Session session = sessionBuilder()
                .build();
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "ROLLBACK", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor);

        try {
            try {
                new RollbackTask().execute(new Rollback(), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()).join();
                fail();
            }
            catch (CompletionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_IN_TRANSACTION.toErrorCode());
        }
        assertFalse(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId());
        assertFalse(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
    }

    @Test
    public void testUnknownTransactionRollback()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);

        Session session = sessionBuilder()
                .setTransactionId(TransactionId.create()) // Use a random transaction ID that is unknown to the system
                .build();
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "ROLLBACK", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor);

        new RollbackTask().execute(new Rollback(), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()).join();
        assertTrue(stateMachine.getQueryInfoWithoutDetails().isClearTransactionId()); // Still issue clear signal
        assertFalse(stateMachine.getQueryInfoWithoutDetails().getStartedTransactionId().isPresent());

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
    }

    private static SessionBuilder sessionBuilder()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME);
    }
}
