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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestDeallocateTask
{
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s")); // TODO: rename

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testDeallocate()
    {
        String statementName = "my_query";
        String query1 = "SELECT bar, baz from foo";
        Session session = TEST_SESSION.withPreparedStatement(statementName, query1);

        String sqlString = "DEALLOCATE PREPARE my_query";
        QueryStateMachine stateMachine = executeDeallocate(statementName, sqlString, session);
        assertEquals(stateMachine.getDeallocatedPreparedStatements(), ImmutableSet.of(statementName));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testDeallocateNoSuchStatement()
    {
        String statementName = "my_query";
        String sqlString = "DEALLOCATE PREPARE my_query";
        executeDeallocate(statementName, sqlString, TEST_SESSION);
    }

    private QueryStateMachine executeDeallocate(String statementName, String sqlString, Session session)
    {
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), sqlString, session, URI.create("fake://uri"), false, transactionManager, executor);
        Deallocate deallocate = new Deallocate(statementName);
        new DeallocateTask().execute(deallocate, transactionManager, metadata, new AllowAllAccessControl(), stateMachine);
        return stateMachine;
    }
}
