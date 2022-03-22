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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.TaskTestUtils.createQueryStateMachine;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestCreateFunctionTask
{
    private final MetadataManager metadataManager = createTestMetadataManager();
    private final ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test
    public void testCreateTemporaryFunction()
    {
        SqlParser parser = new SqlParser();
        String sqlString = "CREATE TEMPORARY FUNCTION foo() RETURNS int RETURN 1";
        CreateFunction statement = (CreateFunction) parser.createStatement(sqlString, ParsingOptions.builder().build());
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine(sqlString, TEST_SESSION, false, transactionManager, executorService, metadataManager);
        new CreateFunctionTask(parser).execute(statement, transactionManager, metadataManager, new AllowAllAccessControl(), stateMachine, emptyList());
        assertEquals(stateMachine.getAddedSessionFunctions().size(), 1);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Session function .* has already been defined")
    public void testCreateTemporaryFunctionWithSameNameFails()
    {
        SqlParser parser = new SqlParser();
        String sqlString = "CREATE TEMPORARY FUNCTION foo() RETURNS int RETURN 1";
        CreateFunction statement = (CreateFunction) parser.createStatement(sqlString, ParsingOptions.builder().build());
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine(sqlString, TEST_SESSION, false, transactionManager, executorService, metadataManager);
        new CreateFunctionTask(parser).execute(statement, transactionManager, metadataManager, new AllowAllAccessControl(), stateMachine, emptyList());
        new CreateFunctionTask(parser).execute(statement, transactionManager, metadataManager, new AllowAllAccessControl(), stateMachine, emptyList());
    }
}
