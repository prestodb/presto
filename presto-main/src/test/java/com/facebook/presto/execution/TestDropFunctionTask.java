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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.TaskTestUtils.createQueryStateMachine;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDropFunctionTask
{
    private final MetadataManager metadataManager = createTestMetadataManager();
    private final ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test
    public void testDropTemporaryFunction()
    {
        String sqlString = "DROP TEMPORARY FUNCTION foo()";
        SqlFunctionId signature = new SqlFunctionId(QualifiedObjectName.valueOf("presto.session.foo"), emptyList());
        Session session = testSessionBuilder()
                .addSessionFunction(signature, null)
                .build();
        Set<SqlFunctionId> removedSessionFunctions = executeAndGetRemovedSessionFunctions(sqlString, session);
        assertTrue(removedSessionFunctions.contains(signature));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Session function .* not found")
    public void testDropMissingTemporaryFunctionFails()
    {
        String sqlString = "DROP TEMPORARY FUNCTION foo()";
        Set<SqlFunctionId> removedSessionFunctions = executeAndGetRemovedSessionFunctions(sqlString, TEST_SESSION);
        assertEquals(removedSessionFunctions.size(), 0);
    }

    @Test
    public void testDropMissingTemporaryFunctionIfExistsFailsSilently()
    {
        String sqlString = "DROP TEMPORARY FUNCTION IF EXISTS foo()";
        Set<SqlFunctionId> removedSessionFunctions = executeAndGetRemovedSessionFunctions(sqlString, TEST_SESSION);
        assertEquals(removedSessionFunctions.size(), 0);
    }

    private Set<SqlFunctionId> executeAndGetRemovedSessionFunctions(String sqlString, Session session)
    {
        SqlParser parser = new SqlParser();
        DropFunction statement = (DropFunction) parser.createStatement(sqlString, ParsingOptions.builder().build());
        TransactionManager tm = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine(sqlString, session, false, tm, executorService, metadataManager);
        new DropFunctionTask(parser).execute(statement, tm, metadataManager, new AllowAllAccessControl(), stateMachine, emptyList());
        return stateMachine.getRemovedSessionFunctions();
    }
}
